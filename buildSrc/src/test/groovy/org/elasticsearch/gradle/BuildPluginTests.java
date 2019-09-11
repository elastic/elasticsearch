/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;


public class BuildPluginTests extends GradleUnitTestCase {

    public void testPassingDockerVersions() {
        BuildPlugin.checkDockerVersionRecent("Docker version 18.06.1-ce, build e68fc7a215d7");
        BuildPlugin.checkDockerVersionRecent("Docker version 17.05.0, build e68fc7a");
        BuildPlugin.checkDockerVersionRecent("Docker version 17.05.1, build e68fc7a");
    }

    @Test(expected = GradleException.class)
    public void testFailingDockerVersions() {
        BuildPlugin.checkDockerVersionRecent("Docker version 17.04.0, build e68fc7a");
    }

    @Test(expected = GradleException.class)
    public void testRepositoryURIThatUsesHttpScheme() throws URISyntaxException {
        final URI uri = new URI("http://s3.amazonaws.com/artifacts.elastic.co/maven");
        BuildPlugin.assertRepositoryURIIsSecure("test", "test", uri);
    }

    public void testRepositoryThatUsesFileScheme() throws URISyntaxException {
        final URI uri = new URI("file:/tmp/maven");
        BuildPlugin.assertRepositoryURIIsSecure("test", "test", uri);
    }

    public void testRepositoryURIThatUsesHttpsScheme() throws URISyntaxException {
        final URI uri = new URI("https://s3.amazonaws.com/artifacts.elastic.co/maven");
        BuildPlugin.assertRepositoryURIIsSecure("test", "test", uri);
    }

    public void testRepositoryURIThatUsesS3Scheme() throws URISyntaxException {
        final URI uri = new URI("s3://artifacts.elastic.co/maven");
        BuildPlugin.assertRepositoryURIIsSecure("test", "test", uri);
    }

}
