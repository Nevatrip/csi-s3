package mounter

import (
	"fmt"
	"os"
	"path"

	"github.com/ctrox/csi-s3/pkg/s3"
)

// Implements Mounter
type rcloneMounter struct {
	meta            *s3.FSMeta
	url             string
	region          string
	accessKeyID     string
	secretAccessKey string
	cfg             *s3.Config
}

const (
	rcloneCmd = "rclone"
)

func newRcloneMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &rcloneMounter{
		meta:            meta,
		url:             cfg.Endpoint,
		region:          cfg.Region,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
		cfg:             cfg,
	}, nil
}

func (rclone *rcloneMounter) Stage(stageTarget string) error {
	return nil
}

func (rclone *rcloneMounter) Unstage(stageTarget string) error {
	return nil
}

func (rclone *rcloneMounter) Mount(source string, target string) error {
	vfsMode := "writes"
	vfsMaxSize := "1G"
	vfsMaxAge := "12h"
	timeout := "1m"
	contimeout := "30s"
	retries := 5

	if rclone.cfg != nil {
		if rclone.cfg.RcloneVfsCacheMode != "" {
			vfsMode = rclone.cfg.RcloneVfsCacheMode
		}
		if rclone.cfg.RcloneVfsCacheMaxSize != "" {
			vfsMaxSize = rclone.cfg.RcloneVfsCacheMaxSize
		}
		if rclone.cfg.RcloneVfsCacheMaxAge != "" {
			vfsMaxAge = rclone.cfg.RcloneVfsCacheMaxAge
		}
		if rclone.cfg.RcloneTimeout != "" {
			timeout = rclone.cfg.RcloneTimeout
		}
		if rclone.cfg.RcloneContimeout != "" {
			contimeout = rclone.cfg.RcloneContimeout
		}
		if rclone.cfg.RcloneRetries != 0 {
			retries = rclone.cfg.RcloneRetries
		}
	}

	args := []string{
		"mount",
		fmt.Sprintf(":s3:%s", path.Join(rclone.meta.BucketName, rclone.meta.Prefix, rclone.meta.FSPath)),
		fmt.Sprintf("%s", target),
		"--daemon",
		"--s3-provider=AWS",
		"--s3-env-auth=true",
		fmt.Sprintf("--s3-region=%s", rclone.region),
		fmt.Sprintf("--s3-endpoint=%s", rclone.url),
		"--allow-other",
		fmt.Sprintf("--vfs-cache-mode=%s", vfsMode),
		fmt.Sprintf("--vfs-cache-max-size=%s", vfsMaxSize),
		fmt.Sprintf("--vfs-cache-max-age=%s", vfsMaxAge),
		fmt.Sprintf("--timeout=%s", timeout),
		fmt.Sprintf("--contimeout=%s", contimeout),
		fmt.Sprintf("--retries=%d", retries),
	}

	if rclone.meta.Gid != 0 {
		args = append(args, fmt.Sprintf("--gid=%d", rclone.meta.Gid))
	}
	if rclone.meta.Uid != 0 {
		args = append(args, fmt.Sprintf("--uid=%d", rclone.meta.Uid))
	}

	os.Setenv("AWS_ACCESS_KEY_ID", rclone.accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", rclone.secretAccessKey)
	return fuseMount(target, rcloneCmd, args)
}
