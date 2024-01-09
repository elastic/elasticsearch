/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.PushResponseItem;

import org.testcontainers.containers.GenericContainer;

public class TestContainerUtils {

    public static void pushForArch(GenericContainer<?> container, String repository) {
        tagAndPush(container, repository, System.getProperty("os.arch"));
    }

    public static void tagAndPush(GenericContainer<?> container, String repository, String tag) {
        // Get the DockerClient used by the Testcontainer library (you can also use your own if they every make that private).
        final DockerClient dockerClient = container.getDockerClient();
        dockerClient.commitCmd(container.getContainerId()).withRepository(repository).withTag(tag).exec();

        // Push new image to your repository. (equivalent to command line 'docker push')
        try {
            String pushImageId = repository + ":" + tag;
            dockerClient.pushImageCmd(pushImageId).exec(new ResultCallback.Adapter<>() {
                @Override
                public void onNext(PushResponseItem object) {
                    if (object.isErrorIndicated()) {
                        // This is just to fail the build in case push of new image fails
                        throw new RuntimeException("Failed push: " + object.getErrorDetail());
                    }
                }
            }).awaitCompletion();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
