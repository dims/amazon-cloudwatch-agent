// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package k8sclient

import (
	"log"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type PodClient interface {
	NamespaceToRunningPodNum() map[string]int
	Init(client kubernetes.Interface)
	Shutdown()
}

type podClient struct {
	sync.RWMutex

	stopChan        chan struct{}
	informerFactory informers.SharedInformerFactory
	podInformer     cache.SharedIndexInformer
	queue           workqueue.RateLimitingInterface

	inited                      bool
	namespaceToRunningPodNumMap map[string]int
}

func (c *podClient) NamespaceToRunningPodNum() map[string]int {
	c.RLock()
	defer c.RUnlock()
	return c.namespaceToRunningPodNumMap
}

func (c *podClient) Init(client kubernetes.Interface) {
	c.Lock()
	defer c.Unlock()
	if c.inited {
		return
	}

	c.namespaceToRunningPodNumMap = make(map[string]int)
	c.stopChan = make(chan struct{})
	c.informerFactory = informers.NewSharedInformerFactory(client, 10*time.Minute)
	c.podInformer = c.informerFactory.Core().V1().Pods().Informer()
	c.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	_, err := c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handlePodAdd,
		UpdateFunc: c.handlePodUpdate,
		DeleteFunc: c.handlePodDelete,
	})
	if err != nil {
		log.Printf("W! Pod unable to add event handler: %v", err)
		return
	}

	go c.informerFactory.Start(c.stopChan)

	if !cache.WaitForCacheSync(c.stopChan, c.podInformer.HasSynced) {
		log.Printf("W! Pod initial sync timeout")
	}

	c.inited = true
}

func (c *podClient) Shutdown() {
	c.Lock()
	defer c.Unlock()
	if !c.inited {
		return
	}

	close(c.stopChan)
	c.queue.ShutDown()

	c.inited = false
}

func (c *podClient) handlePodAdd(obj interface{}) {
	c.updatePodCount(obj.(*v1.Pod), 1)
}

func (c *podClient) handlePodUpdate(oldObj, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)

	if oldPod.Status.Phase != v1.PodRunning && newPod.Status.Phase == v1.PodRunning {
		c.updatePodCount(newPod, 1)
	} else if oldPod.Status.Phase == v1.PodRunning && newPod.Status.Phase != v1.PodRunning {
		c.updatePodCount(newPod, -1)
	}
}

func (c *podClient) handlePodDelete(obj interface{}) {
	c.updatePodCount(obj.(*v1.Pod), -1)
}

func (c *podClient) updatePodCount(pod *v1.Pod, delta int) {
	if pod.Status.Phase == v1.PodRunning {
		c.namespaceToRunningPodNumMap[pod.Namespace] += delta
		if c.namespaceToRunningPodNumMap[pod.Namespace] <= 0 {
			delete(c.namespaceToRunningPodNumMap, pod.Namespace)
		}
	}
}
