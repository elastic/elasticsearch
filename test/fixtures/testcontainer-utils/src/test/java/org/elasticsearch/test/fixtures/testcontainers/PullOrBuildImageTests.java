/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PullImageCmd;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;

import org.junit.Test;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PullOrBuildImageTests {

    @Test
    public void testImageExistsLocally_SkipsPull() throws Exception {
        // Setup mocks
        DockerClient dockerClient = mock(DockerClient.class);
        ImageFromDockerfile fallbackImage = mock(ImageFromDockerfile.class);
        ImagePullPolicy pullPolicy = mock(ImagePullPolicy.class);

        String remoteImage = "registry.example.com/myimage:latest";
        DockerImageName imageName = DockerImageName.parse(remoteImage);

        // Image exists locally - shouldPull returns false
        when(pullPolicy.shouldPull(imageName)).thenReturn(false);

        // Create PullOrBuildImage with mocked dependencies
        PullOrBuildImage image = new PullOrBuildImage(imageName, fallbackImage, () -> dockerClient, pullPolicy);

        // Execute
        String result = image.get();

        // Verify
        assertEquals(remoteImage, result);
        // Verify pull was never attempted
        verify(dockerClient, never()).pullImageCmd(any());
        verify(fallbackImage, never()).get();
    }

    @Test
    public void testPullSucceeds() throws Exception {
        // Setup mocks
        DockerClient dockerClient = mock(DockerClient.class);
        PullImageCmd pullImageCmd = mock(PullImageCmd.class);
        PullImageResultCallback callback = mock(PullImageResultCallback.class);
        ImageFromDockerfile fallbackImage = mock(ImageFromDockerfile.class);
        ImagePullPolicy pullPolicy = mock(ImagePullPolicy.class);

        String remoteImage = "registry.example.com/myimage:latest";
        DockerImageName imageName = DockerImageName.parse(remoteImage);

        // Image does not exist locally - shouldPull returns true
        when(pullPolicy.shouldPull(imageName)).thenReturn(true);
        when(dockerClient.pullImageCmd(remoteImage)).thenReturn(pullImageCmd);
        when(pullImageCmd.exec(any(PullImageResultCallback.class))).thenReturn(callback);
        when(callback.awaitCompletion()).thenReturn(callback);

        // Create PullOrBuildImage with mocked dependencies
        PullOrBuildImage image = new PullOrBuildImage(imageName, fallbackImage, () -> dockerClient, pullPolicy);

        // Execute
        String result = image.get();

        // Verify
        assertEquals(remoteImage, result);
        verify(dockerClient).pullImageCmd(remoteImage);
        verify(fallbackImage, never()).get();
    }

    @Test
    public void testPullFailsWithNotFound_FallsBackToBuild() throws Exception {
        // Setup mocks
        DockerClient dockerClient = mock(DockerClient.class);
        PullImageCmd pullImageCmd = mock(PullImageCmd.class);
        ImageFromDockerfile fallbackImage = mock(ImageFromDockerfile.class);
        ImagePullPolicy pullPolicy = mock(ImagePullPolicy.class);

        String remoteImage = "registry.example.com/nonexistent:latest";
        DockerImageName imageName = DockerImageName.parse(remoteImage);
        String builtImageId = "sha256:abc123";

        // Image does not exist locally - shouldPull returns true
        when(pullPolicy.shouldPull(imageName)).thenReturn(true);
        when(dockerClient.pullImageCmd(remoteImage)).thenReturn(pullImageCmd);
        when(pullImageCmd.exec(any(PullImageResultCallback.class))).thenThrow(new NotFoundException("Image not found"));
        when(fallbackImage.get()).thenReturn(builtImageId);

        // Create PullOrBuildImage with mocked dependencies
        PullOrBuildImage image = new PullOrBuildImage(imageName, fallbackImage, () -> dockerClient, pullPolicy);

        // Execute
        String result = image.get();

        // Verify
        assertEquals(builtImageId, result);
        verify(dockerClient).pullImageCmd(remoteImage);
        verify(fallbackImage).get();
    }

    @Test
    public void testPullFailsWithGenericException_FallsBackToBuild() throws Exception {
        // Setup mocks
        DockerClient dockerClient = mock(DockerClient.class);
        PullImageCmd pullImageCmd = mock(PullImageCmd.class);
        ImageFromDockerfile fallbackImage = mock(ImageFromDockerfile.class);
        ImagePullPolicy pullPolicy = mock(ImagePullPolicy.class);

        String remoteImage = "registry.example.com/myimage:latest";
        DockerImageName imageName = DockerImageName.parse(remoteImage);
        String builtImageId = "sha256:def456";

        // Image does not exist locally - shouldPull returns true
        when(pullPolicy.shouldPull(imageName)).thenReturn(true);
        when(dockerClient.pullImageCmd(remoteImage)).thenReturn(pullImageCmd);
        when(pullImageCmd.exec(any(PullImageResultCallback.class))).thenThrow(new RuntimeException("Network error"));
        when(fallbackImage.get()).thenReturn(builtImageId);

        // Create PullOrBuildImage with mocked dependencies
        PullOrBuildImage image = new PullOrBuildImage(imageName, fallbackImage, () -> dockerClient, pullPolicy);

        // Execute
        String result = image.get();

        // Verify
        assertEquals(builtImageId, result);
        verify(dockerClient).pullImageCmd(remoteImage);
        verify(fallbackImage).get();
    }

    @Test
    public void testPullFailsWithInterruptedException_FallsBackToBuild() throws Exception {
        // Setup mocks
        DockerClient dockerClient = mock(DockerClient.class);
        PullImageCmd pullImageCmd = mock(PullImageCmd.class);
        PullImageResultCallback callback = mock(PullImageResultCallback.class);
        ImageFromDockerfile fallbackImage = mock(ImageFromDockerfile.class);
        ImagePullPolicy pullPolicy = mock(ImagePullPolicy.class);

        String remoteImage = "registry.example.com/myimage:latest";
        DockerImageName imageName = DockerImageName.parse(remoteImage);
        String builtImageId = "sha256:ghi789";

        // Image does not exist locally - shouldPull returns true
        when(pullPolicy.shouldPull(imageName)).thenReturn(true);
        when(dockerClient.pullImageCmd(remoteImage)).thenReturn(pullImageCmd);
        when(pullImageCmd.exec(any(PullImageResultCallback.class))).thenReturn(callback);
        when(callback.awaitCompletion()).thenThrow(new InterruptedException("Pull interrupted"));
        when(fallbackImage.get()).thenReturn(builtImageId);

        // Create PullOrBuildImage with mocked dependencies
        PullOrBuildImage image = new PullOrBuildImage(imageName, fallbackImage, () -> dockerClient, pullPolicy);

        // Execute
        String result = image.get();

        // Verify fallback was used
        assertEquals(builtImageId, result);
        verify(fallbackImage).get();
    }
}
