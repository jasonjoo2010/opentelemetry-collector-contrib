// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	k8s_testing "k8s.io/client-go/testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sinventory"
)

func setResourceVersionRetryDelay(observer *Observer, delay time.Duration) {
	observer.resourceVersionRetryBackoff.Duration = delay
	observer.resourceVersionRetryBackoff.Jitter = 0
}

func TestObserver(t *testing.T) {
	mockClient := newMockDynamicClient()
	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{
			"environment": "production",
		}, "1"),
	)

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{"default"},
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	mockClient.createPods(
		generatePod("pod2", "default", map[string]any{
			"environment": "test",
		}, "2"),
		generatePod("pod3", "default_ignore", map[string]any{
			"environment": "production",
		}, "3"),
		generatePod("pod4", "default", map[string]any{
			"environment": "production",
		}, "4"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverWithInitialState(t *testing.T) {
	mockClient := newMockDynamicClient()
	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{
			"environment": "production",
		}, "1"),
	)

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{"default"},
		},
		IncludeInitialState: true,
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	verifyReceivedEvents(t, 1, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverExcludeDelete(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{"default"},
		},
		IncludeInitialState: true,
		Exclude: map[apiWatch.EventType]bool{
			apiWatch.Deleted: true,
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	<-time.After(time.Millisecond * 100)

	pod := generatePod("pod1", "default", map[string]any{
		"environment": "production",
	}, "1")

	// create and delete the pod - only the creation event should be received
	mockClient.createPods(pod)
	mockClient.deletePods(pod)

	verifyReceivedEvents(t, 1, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverEmptyNamespaces(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{}, // empty to watch all namespaces
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{"env": "test"}, "1"),
		generatePod("pod2", "other", map[string]any{"env": "prod"}, "2"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverMultipleNamespaces(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{"default", "other"},
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{"env": "test"}, "1"),
		generatePod("pod2", "other", map[string]any{"env": "prod"}, "2"),
		generatePod("pod3", "ignored", map[string]any{"env": "dev"}, "3"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverWithSelectors(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces:      []string{"default"},
			LabelSelector:   "environment=test",
			FieldSelector:   "",
			ResourceVersion: "",
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	// Since fake client doesn't filter, it will return all, but the code path is covered
	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{"environment": "test"}, "1"),
		generatePod("pod2", "default", map[string]any{"environment": "prod"}, "2"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverInitialStateError(t *testing.T) {
	mockClient := newMockDynamicClient()

	// Make list return error for initial state
	mockClient.client.(*fake.FakeDynamicClient).PrependReactor("list", "pods", func(_ k8s_testing.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("mock list error")
	})

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{"default"},
		},
		IncludeInitialState: true,
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	// No events should be received due to error
	select {
	case <-receivedEventsChan:
		t.Fatal("unexpected event received")
	case <-time.After(100 * time.Millisecond):
		// ok
	}

	close(stopChan)

	wg.Wait()
}

func TestObserverInitialStateNoObjects(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			Namespaces: []string{"default"},
		},
		IncludeInitialState: true,
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	// No events since no objects
	select {
	case <-receivedEventsChan:
		t.Fatal("unexpected event received")
	case <-time.After(100 * time.Millisecond):
		// ok
	}

	close(stopChan)

	wg.Wait()
}

func TestObserverRetriesResourceVersionRelistPastBackoffCap(t *testing.T) {
	scheme := runtime.NewScheme()
	gvrToListKind := map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "pods"}: "PodList",
	}
	fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)

	listCalls := 0
	fakeClient.PrependReactor("list", "pods", func(_ k8s_testing.Action) (bool, runtime.Object, error) {
		listCalls++
		switch listCalls {
		case 1:
			return true, podListWithResourceVersion("100"), nil
		case 2, 3, 4, 5:
			return true, nil, errors.New("transient list error")
		default:
			return true, podListWithResourceVersion("200"), nil
		}
	})

	watchResourceVersions := make(chan string, 2)
	watchCalls := 0
	fakeClient.PrependWatchReactor("pods", func(action k8s_testing.Action) (bool, apiWatch.Interface, error) {
		watchCalls++
		watchAction := action.(k8s_testing.WatchAction)
		watchResourceVersions <- watchAction.GetWatchRestrictions().ResourceVersion

		watcher := apiWatch.NewFakeWithChanSize(1, false)
		switch watchCalls {
		case 1:
			go watcher.Error(resourceVersionExpiredStatus())
		case 2:
			go watcher.Add(generatePod("pod-after-relist", "default", nil, "201"))
		}
		return true, watcher, nil
	})

	cfg := Config{
		Config: k8sinventory.Config{
			Gvr:        schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			Namespaces: []string{"default"},
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event, 1)
	obs, err := New(mockDynamicClient{client: fakeClient}, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})
	require.NoError(t, err)
	setResourceVersionRetryDelay(obs, time.Millisecond)
	obs.resourceVersionRetryBackoff.Cap = 2 * time.Millisecond

	wg := sync.WaitGroup{}
	stopChan := obs.Start(t.Context(), &wg)

	select {
	case event := <-receivedEventsChan:
		assert.Equal(t, apiWatch.Added, event.Type)
	case <-time.After(2 * time.Second):
		close(stopChan)
		wg.Wait()
		t.Fatal("timeout waiting for event after relist retry")
	}

	close(stopChan)
	wg.Wait()

	assert.Equal(t, "100", <-watchResourceVersions)
	assert.Equal(t, "200", <-watchResourceVersions)
	assert.GreaterOrEqual(t, listCalls, 6)
}

func TestResourceVersionRetryStopsPromptly(t *testing.T) {
	scheme := runtime.NewScheme()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{gvr: "PodList"},
	)

	listCalled := make(chan struct{}, 1)
	fakeClient.PrependReactor("list", "pods", func(_ k8s_testing.Action) (bool, runtime.Object, error) {
		listCalled <- struct{}{}
		return true, nil, errors.New("persistent list error")
	})

	obs, err := New(
		mockDynamicClient{client: fakeClient},
		Config{Config: k8sinventory.Config{Gvr: gvr}},
		zap.NewNop(),
		nil,
	)
	require.NoError(t, err)
	setResourceVersionRetryDelay(obs, time.Hour)

	stopperChan := make(chan struct{})
	retryDone := make(chan error, 1)
	go func() {
		_, err := obs.getResourceVersionWithRetry(
			t.Context(),
			k8sinventory.Config{Gvr: gvr},
			fakeClient.Resource(gvr),
			stopperChan,
		)
		retryDone <- err
	}()

	<-listCalled
	close(stopperChan)

	select {
	case err := <-retryDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("resourceVersion retry did not stop promptly")
	}
}

func TestObserverNonStatusWatchErrorDoesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.False(t, isExpiredResourceVersionEvent(apiWatch.Event{
			Type:   apiWatch.Error,
			Object: generatePod("not-a-status", "default", nil, "1"),
		}))
	})
}

func verifyReceivedEvents(t *testing.T, numEvents int, receivedEventsChan chan *apiWatch.Event, stopChan chan struct{}) {
	receivedEvents := 0

	exit := false
	for {
		select {
		case <-receivedEventsChan:
			receivedEvents++
			if receivedEvents == numEvents {
				exit = true
			}
		case <-time.After(10 * time.Second):
			t.Log("timed out waiting for expected events")
			t.Fail()
			exit = true
		}
		if exit {
			break
		}
	}

	close(stopChan)
}

type mockDynamicClient struct {
	client dynamic.Interface
}

func (c mockDynamicClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return c.client.Resource(resource)
}

func newMockDynamicClient() mockDynamicClient {
	scheme := runtime.NewScheme()
	objs := []runtime.Object{}

	gvrToListKind := map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "pods"}: "PodList",
	}

	fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objs...)
	return mockDynamicClient{
		client: fakeClient,
	}
}

func (c mockDynamicClient) createPods(objects ...*unstructured.Unstructured) {
	pods := c.client.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})
	for _, pod := range objects {
		_, _ = pods.Namespace(pod.GetNamespace()).Create(context.Background(), pod, v1.CreateOptions{})
	}
}

func (c mockDynamicClient) deletePods(objects ...*unstructured.Unstructured) {
	pods := c.client.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})
	for _, pod := range objects {
		_ = pods.Namespace(pod.GetNamespace()).Delete(context.Background(), pod.GetName(), v1.DeleteOptions{})
	}
}

func generatePod(name, namespace string, labels map[string]any, resourceVersion string) *unstructured.Unstructured {
	pod := unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pods",
			"metadata": map[string]any{
				"namespace": namespace,
				"name":      name,
				"labels":    labels,
			},
		},
	}

	pod.SetResourceVersion(resourceVersion)
	return &pod
}

func podListWithResourceVersion(resourceVersion string) *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetResourceVersion(resourceVersion)
	return list
}

func resourceVersionExpiredStatus() *v1.Status {
	return &v1.Status{
		Status:  v1.StatusFailure,
		Reason:  v1.StatusReasonExpired,
		Message: "The resourceVersion for the provided watch is too old.",
		Code:    http.StatusGone,
	}
}
