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
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

// makeMountArgs makes the arguments to the mount(8) command.
func makeMountArgs(source, target, fstype string, options []string) []string {
	// Build mount command as follows:
	//   mount [-t $fstype] [-o $options] [$source] $target
	mountArgs := []string{}
	if len(fstype) > 0 {
		mountArgs = append(mountArgs, "-t", fstype)
	}
	if len(options) > 0 {
		mountArgs = append(mountArgs, "-o", strings.Join(options, ","))
	}
	if len(source) > 0 {
		mountArgs = append(mountArgs, source)
	}
	mountArgs = append(mountArgs, target)

	return mountArgs
}

func mount(source string, target string, fstype string, options []string) error {
	mountArgs := makeMountArgs(source, target, fstype, options)
	command := exec.Command("mount", mountArgs...)
	_, err := command.CombinedOutput()
	return err
}

func umount(target string) error {
	command := exec.Command("umount", target)
	_, err := command.CombinedOutput()
	return err
}

func TestEvalSymlinksAtRoot(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skipf("test needs root permission, skipped")
		return
	}

	tmpDir, err := ioutil.TempDir("", "tmpfs-for-evalsymlinksatroot")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	err = mount("tmpfs", tmpDir, "tmpfs", []string{"size=1m"})
	if err != nil {
		t.Fatal(err)
	}
	defer umount(tmpDir)

	err = os.MkdirAll(filepath.Join(tmpDir, "foo"), 0750)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(tmpDir, "foo/baz"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// one level absolute link file
	err = os.Symlink(filepath.Join(tmpDir, "foo/baz"), filepath.Join(tmpDir, "foo/bar"))
	if err != nil {
		t.Fatal(err)
	}
	// one level relative link file
	err = os.Symlink("baz", filepath.Join(tmpDir, "foo/relbar"))
	if err != nil {
		t.Fatal(err)
	}
	// two level link file
	err = os.Symlink(filepath.Join(tmpDir, "foo/bar"), filepath.Join(tmpDir, "foo/twolevel"))
	if err != nil {
		t.Fatal(err)
	}
	// two level relative link file
	err = os.Symlink("relbar", filepath.Join(tmpDir, "foo/reltwolevel"))
	if err != nil {
		t.Fatal(err)
	}
	// absolute link dir
	err = os.Symlink(filepath.Join(tmpDir, "foo"), filepath.Join(tmpDir, "dir"))
	if err != nil {
		t.Fatal(err)
	}
	// relative link dir
	err = os.Symlink(filepath.Join(tmpDir, "foo"), filepath.Join(tmpDir, "reldir"))
	if err != nil {
		t.Fatal(err)
	}
	// symbolic link to does-not-exist path in specified filesystem
	err = os.Symlink(filepath.Join(tmpDir, "does-not-exist"), filepath.Join(tmpDir, "foo/invalid"))
	if err != nil {
		t.Fatal(err)
	}
	// symbolic link to does-not-exist path in specified filesystem but exist in root filesystem ("/")
	err = os.Symlink("/etc/hosts", filepath.Join(tmpDir, "etc_hosts"))
	if err != nil {
		t.Fatal(err)
	}

	tmpNewRoot, err := ioutil.TempDir("", "tmp-fsroot")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpNewRoot)
	err = mount(tmpDir, tmpNewRoot, "", []string{"bind", "ro"})
	if err != nil {
		t.Fatal(err)
	}
	defer umount(tmpNewRoot)

	testcases := []struct {
		name         string
		path         string
		expectedPath string
		expectedErr  error
	}{
		{
			"one-level-link",
			"/foo/bar",
			"/foo/baz",
			nil,
		},
		{
			"two-level-link",
			"/foo/twolevel",
			"/foo/baz",
			nil,
		},
		{
			"one-level-relative-link",
			"/foo/relbar",
			"/foo/baz",
			nil,
		},
		{
			"two-level-relative-link",
			"/foo/reltwolevel",
			"/foo/baz",
			nil,
		},
		{
			"one-level-link-under-link-dir",
			"/dir/bar",
			"/foo/baz",
			nil,
		},
		{
			"two-level-link-under-link-dir",
			"/dir/twolevel",
			"/foo/baz",
			nil,
		},
		{
			"one-level-relative-link-under-link-dir",
			"/dir/relbar",
			"/foo/baz",
			nil,
		},
		{
			"two-level-relative-link-under-link-dir",
			"/dir/reltwolevel",
			"/foo/baz",
			nil,
		},
		{
			"one-level-link-under-link-reldir",
			"/reldir/bar",
			"/foo/baz",
			nil,
		},
		{
			"two-level-link-under-link-reldir",
			"/reldir/twolevel",
			"/foo/baz",
			nil,
		},
		{
			"one-level-relative-link-under-link-reldir",
			"/reldir/relbar",
			"/foo/baz",
			nil,
		},
		{
			"two-level-relative-link-under-link-reldir",
			"/reldir/reltwolevel",
			"/foo/baz",
			nil,
		},
		{
			"link-to-invalid-path",
			"/foo/invalid",
			"",
			&os.PathError{Op: "lstat", Path: filepath.Join(tmpNewRoot, "does-not-exist"), Err: syscall.ENOENT},
		},
		{
			"link-to-path-which-exists-in-caller-fs",
			"/etc_hosts",
			"",
			&os.PathError{Op: "readlink", Path: "/etc/hosts", Err: os.ErrNotExist},
		},
	}
	for _, v := range testcases {
		path, err := EvalSymlinksAtNewRoot(v.path, tmpDir, tmpNewRoot)
		if path != v.expectedPath {
			t.Errorf("test %s: expected path %v, got %v", v.name, v.expectedPath, path)
		}
		if !reflect.DeepEqual(err, v.expectedErr) {
			t.Errorf("test %s: expected error %#v, got %#v", v.name, v.expectedErr, err)
		}
	}
}
