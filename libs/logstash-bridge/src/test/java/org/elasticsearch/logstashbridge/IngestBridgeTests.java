/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.script.Metadata;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.logstashbridge.DucktypeMatchers.instancesQuackLike;
import static org.elasticsearch.logstashbridge.DucktypeMatchers.staticallyQuacksLike;

public class IngestBridgeTests extends ESTestCase {

    public void testIngestDocumentAPIStability() throws Exception {
        interface IngestDocumentInstanceAPI {
            Metadata getMetadata();

            boolean updateIndexHistory(String index);

            Set<String> getIndexHistory();

            Map<String, Object> getIngestMetadata();

            <T> T getFieldValue(String fieldName, Class<T> type);

            String renderTemplate(TemplateScript.Factory tf);

            void setFieldValue(String fieldName, ValueSource valueSource);

            void removeField(String fieldName);

            void executePipeline(Pipeline pipeline, BiConsumer<IngestDocument, Exception> handler);
        }

        assertThat(IngestDocument.class, instancesQuackLike(IngestDocumentInstanceAPI.class));
    }

    public void testConfigurationUtilsAPIStability() throws Exception {
        interface ConfigurationUtilsStaticAPI {
            boolean readBooleanProperty(String pType, String pTag, Map<String, Object> config, String propertyName, boolean defaultValue);

            String readStringProperty(String pType, String pTag, Map<String, Object> config, String propertyName, String defaultValue);

            TemplateScript.Factory compileTemplate(
                String pType,
                String pTag,
                String propertyName,
                String propertyValue,
                ScriptService scriptService
            );
        }

        assertThat(ConfigurationUtils.class, staticallyQuacksLike(ConfigurationUtilsStaticAPI.class));
    }
}
