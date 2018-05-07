/*
Copyright 2018 The Kubernetes Authors.

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

package nsenter

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// EvalSymlinksAtNewRoot evaluates symbolic links of one filesystem at its new
// mountpoint. It works like filepath.EvalSymlinks except it does not assume
// filesystem root is OS root "/".
// `filepath.EvalSymlinks` equals to `EvalSymlinksAtNewRoot(path, "/", "/")`.
func EvalSymlinksAtNewRoot(path string, oldroot, newroot string) (string, error) {
	return walkSymlinks(path, filepath.Clean(oldroot), filepath.Clean(newroot))
}

func walkSymlinks(path string, oldroot, newroot string) (string, error) {
	if path == "" {
		return path, nil
	}
	var linksWalked int // to protect against cycles
	for {
		i := linksWalked
		newpath, err := walkLinks(path, oldroot, newroot, &linksWalked)
		if err != nil {
			return "", err
		}
		if i == linksWalked {
			return filepath.Clean(newpath), nil
		}
		path = newpath
	}
}

func walkLinks(path string, oldroot, newroot string, linksWalked *int) (string, error) {
	switch dir, file := filepath.Split(path); {
	case dir == "":
		newpath, _, err := walkLink(file, oldroot, newroot, linksWalked)
		return newpath, err
	case file == "":
		if os.IsPathSeparator(dir[len(dir)-1]) {
			if isRoot(dir) {
				return dir, nil
			}
			return walkLinks(dir[:len(dir)-1], oldroot, newroot, linksWalked)
		}
		newpath, _, err := walkLink(dir, oldroot, newroot, linksWalked)
		return newpath, err
	default:
		newdir, err := walkLinks(dir, oldroot, newroot, linksWalked)
		if err != nil {
			return "", err
		}
		newpath, islink, err := walkLink(filepath.Join(newdir, file), oldroot, newroot, linksWalked)
		if err != nil {
			return "", err
		}
		if !islink {
			return newpath, nil
		}
		if filepath.IsAbs(newpath) || os.IsPathSeparator(newpath[0]) {
			return newpath, nil
		}
		return filepath.Join(newdir, newpath), nil
	}
}

func walkLink(path string, oldroot, newroot string, linksWalked *int) (newpath string, islink bool, err error) {
	if *linksWalked > 255 {
		return "", false, errors.New("EvalSymlinks: too many links")
	}
	fi, err := os.Lstat(filepath.Join(newroot, path))
	if err != nil {
		return "", false, err
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return path, false, nil
	}
	newpath, err = os.Readlink(filepath.Join(newroot, path))
	if err != nil {
		return "", false, err
	}
	*linksWalked++
	if filepath.IsAbs(newpath) {
		if strings.HasPrefix(newpath, oldroot) {
			// If it's inside in old filesystem, remove its root prefix.
			newpath = filepath.Clean("/" + strings.TrimPrefix(newpath, oldroot))
		} else {
			// If it's outside of old filesystem, it does not exist
			return "", false, &os.PathError{Op: "readlink", Path: newpath, Err: os.ErrNotExist}
		}
	}
	return newpath, true, nil
}

func isRoot(path string) bool {
	return path == "/"
}
