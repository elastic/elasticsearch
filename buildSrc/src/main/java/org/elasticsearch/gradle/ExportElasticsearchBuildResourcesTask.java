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

import org.gradle.api.DefaultTask;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Export Elasticsearch build resources to configurable paths
 *
 * Wil overwrite existing files and create missing directories.
 * Useful for resources that that need to be passed to other processes trough the filesystem or otherwise can't be
 * consumed from the classpath.
 */
public class ExportElasticsearchBuildResourcesTask extends DefaultTask {

    private final Logger logger =  Logging.getLogger(ExportElasticsearchBuildResourcesTask.class);

    private final Set<ExportPair> resources = new HashSet<>();

    @Input
    @SkipWhenEmpty
    public Set<ExportPair> getResources() {
        return Collections.unmodifiableSet(resources);
    }

    @Classpath
    public String getResourceSourceFiles() {
        logger.info("Classpath: {}", System.getProperty("java.class.path"));
        return System.getProperty("java.class.path");
    }

    @OutputFiles
    public List<File> getOutputFiles() {
        return resources.stream()
            .map(ExportPair::getOutputFile)
            .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    }

    public void resource(Map<ElasticsearchBuildResource, File> resourcesMap) {
        resourcesMap.forEach(this::resource);
    }

    public void resource(ElasticsearchBuildResource resource, File outputFile) {
        resources.add(new ExportPair(resource, outputFile));
    }

    @TaskAction
    public void doExport() {
        if (resources.isEmpty()) {
            throw new StopExecutionException();
        }
        logger.info("exporting resources");
    }

    // Gradle wants this to be Serializable, we achieve this by extending list,
    // also alows for variable expansion in Groovy.
    public static class ExportPair extends ArrayList<Object> {
        private final ElasticsearchBuildResource resource;
        private final File outputFile;

        public ExportPair(ElasticsearchBuildResource resource, File outputFile) {
            this.resource = resource;
            this.outputFile = outputFile;
            super.add(resource);
            super.add(outputFile);
        }

        public ElasticsearchBuildResource getResource() {
            return resource;
        }

        public File getOutputFile() {
            return outputFile;
        }

        @Override
        public boolean add(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, Collection<?> c) {
            throw new UnsupportedOperationException();
        }
    }

}
