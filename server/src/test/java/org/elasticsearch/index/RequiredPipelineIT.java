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

package org.elasticsearch.index;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class RequiredPipelineIT extends ESIntegTestCase {

    public void testRequiredPipeline() {
        final Settings settings = Settings.builder().put(IndexSettings.REQUIRED_PIPELINE.getKey(), "required_pipeline").build();
        createIndex("index", settings);

        // this asserts that the required_pipeline was used, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index", "_doc", "1").setSource(Map.of("field", "value")).get());
        assertThat(e, hasToString(containsString("pipeline with id [required_pipeline] does not exist")));
    }

    public void testDefaultAndRequiredPipeline() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .put(IndexSettings.REQUIRED_PIPELINE.getKey(), "required_pipeline")
            .build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createIndex("index", settings));
        assertThat(
            e,
            hasToString(containsString("index has a default pipeline [default_pipeline] and a required pipeline [required_pipeline]")));
    }

    public void testDefaultAndRequiredPipelineFromTemplates() {
        final int lowOrder = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int highOrder = randomIntBetween(lowOrder + 1, Integer.MAX_VALUE);
        final int requiredPipelineOrder;
        final int defaultPipelineOrder;
        if (randomBoolean()) {
            defaultPipelineOrder = lowOrder;
            requiredPipelineOrder = highOrder;
        } else {
            defaultPipelineOrder = highOrder;
            requiredPipelineOrder = lowOrder;
        }
        final Settings defaultPipelineSettings =
            Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        admin().indices()
            .preparePutTemplate("default")
            .setPatterns(List.of("index*"))
            .setOrder(defaultPipelineOrder)
            .setSettings(defaultPipelineSettings)
            .get();
        final Settings requiredPipelineSettings =
            Settings.builder().put(IndexSettings.REQUIRED_PIPELINE.getKey(), "required_pipeline").build();
        admin().indices()
            .preparePutTemplate("required")
            .setPatterns(List.of("index*"))
            .setOrder(requiredPipelineOrder)
            .setSettings(requiredPipelineSettings)
            .get();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index", "_doc", "1").setSource(Map.of("field", "value")).get());
        assertThat(
            e,
            hasToString(containsString(
                "required pipeline [required_pipeline] and default pipeline [default_pipeline] can not both be set")));
    }

    public void testHighOrderRequiredPipelinePreferred() throws IOException {
        final int lowOrder = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int highOrder = randomIntBetween(lowOrder + 1, Integer.MAX_VALUE);
        final Settings defaultPipelineSettings =
            Settings.builder().put(IndexSettings.REQUIRED_PIPELINE.getKey(), "low_order_required_pipeline").build();
        admin().indices()
            .preparePutTemplate("default")
            .setPatterns(List.of("index*"))
            .setOrder(lowOrder)
            .setSettings(defaultPipelineSettings)
            .get();
        final Settings requiredPipelineSettings =
            Settings.builder().put(IndexSettings.REQUIRED_PIPELINE.getKey(), "high_order_required_pipeline").build();
        admin().indices()
            .preparePutTemplate("required")
            .setPatterns(List.of("index*"))
            .setOrder(highOrder)
            .setSettings(requiredPipelineSettings)
            .get();

        // this asserts that the high_order_required_pipeline was selected, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index", "_doc", "1").setSource(Map.of("field", "value")).get());
        assertThat(e, hasToString(containsString("pipeline with id [high_order_required_pipeline] does not exist")));
    }

    public void testRequiredPipelineAndRequestPipeline() {
        final Settings settings = Settings.builder().put(IndexSettings.REQUIRED_PIPELINE.getKey(), "required_pipeline").build();
        createIndex("index", settings);
        final IndexRequestBuilder builder = client().prepareIndex("index", "_doc", "1");
        builder.setSource(Map.of("field", "value"));
        builder.setPipeline("request_pipeline");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::get);
        assertThat(
            e,
            hasToString(containsString("request pipeline [request_pipeline] can not override required pipeline [required_pipeline]")));
    }

}
