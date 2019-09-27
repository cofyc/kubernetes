/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statefulset

import (
	"encoding/json"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	deletedSlotsAnnotation = "delete-slots"
)

func getDeleteSlots(set *apps.StatefulSet) (deleteSlots sets.Int) {
	deleteSlots = sets.NewInt()
	if set.Annotations == nil {
		return
	}
	value, ok := set.Annotations[deletedSlotsAnnotation]
	if !ok {
		return
	}
	var slice []int
	err := json.Unmarshal([]byte(value), &slice)
	if err != nil {
		return
	}
	deleteSlots.Insert(slice...)
	return
}

func setDeleteSlot(set *apps.StatefulSet, deleteSlots sets.Int) (err error) {
	b, err := json.Marshal(deleteSlots.List())
	if err != nil {
		return
	}
	metav1.SetMetaDataAnnotation(&set.ObjectMeta, deletedSlotsAnnotation, string(b))
	return
}
