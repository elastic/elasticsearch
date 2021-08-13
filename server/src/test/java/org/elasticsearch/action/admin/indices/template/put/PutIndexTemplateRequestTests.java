/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class PutIndexTemplateRequestTests extends ESTestCase {

    public void testValidateErrorMessage() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        ActionRequestValidationException withoutNameAndPattern = request.validate();
        assertThat(withoutNameAndPattern.getMessage(), containsString("name is missing"));
        assertThat(withoutNameAndPattern.getMessage(), containsString("index patterns are missing"));

        request.name("foo");
        ActionRequestValidationException withoutIndexPatterns = request.validate();
        assertThat(withoutIndexPatterns.validationErrors(), hasSize(1));
        assertThat(withoutIndexPatterns.getMessage(), containsString("index patterns are missing"));

        request.patterns(Collections.singletonList("test-*"));
        ActionRequestValidationException noError = request.validate();
        assertThat(noError, is(nullValue()));
    }

    public void testMappingKeyedByType() throws IOException {
        PutIndexTemplateRequest request1 = new PutIndexTemplateRequest("foo");
        PutIndexTemplateRequest request2 = new PutIndexTemplateRequest("bar");
        {
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject().startObject("properties")
                .startObject("field1")
                .field("type", "text")
                .endObject()
                .startObject("field2")
                .startObject("properties")
                .startObject("field21")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject().endObject();
            request1.mapping(builder);
            builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject().startObject("_doc")
                .startObject("properties")
                .startObject("field1")
                .field("type", "text")
                .endObject()
                .startObject("field2")
                .startObject("properties")
                .startObject("field21")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject().endObject();
            request2.mapping(builder);
            assertEquals(request1.mappings(), request2.mappings());
        }
        {
            request1 = new PutIndexTemplateRequest("foo");
            request2 = new PutIndexTemplateRequest("bar");
            String nakedMapping = "{\"properties\": {\"foo\": {\"type\": \"integer\"}}}";
            request1.mapping(nakedMapping, XContentType.JSON);
            request2.mapping("{\"_doc\": " + nakedMapping + "}", XContentType.JSON);
            assertEquals(request1.mappings(), request2.mappings());
        }
        {
            request1 = new PutIndexTemplateRequest("foo");
            request2 = new PutIndexTemplateRequest("bar");
            Map<String, Object> nakedMapping = MapBuilder.<String, Object>newMapBuilder()
                .put("properties", MapBuilder.<String, Object>newMapBuilder()
                    .put("bar", MapBuilder.<String, Object>newMapBuilder()
                        .put("type", "scaled_float")
                        .put("scaling_factor", 100)
                        .map())
                    .map())
                .map();
            request1.mapping(nakedMapping);
            request2.mapping(Map.of("_doc", nakedMapping));
            assertEquals(request1.mappings(), request2.mappings());
        }
    }

    public void testSourceParsing() throws IOException {
        XContentBuilder indexPatterns = XContentFactory.jsonBuilder().startObject()
            .array("index_patterns", "index-*", "other-index-*")
            .field("version", 2)
            .field("order", 5)
            .startObject("settings")
                .field("index.refresh_interval", "-1")
                .field("index.number_of_replicas", 2)
            .endObject()
            .startObject("mappings")
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .startObject("aliases")
                .startObject("my-alias").endObject()
            .endObject()
        .endObject();

        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(indexPatterns);

        assertThat(request.patterns(), containsInAnyOrder("index-*", "other-index-*"));
        assertThat(request.version(), equalTo(2));
        assertThat(request.order(), equalTo(5));

        Settings settings = Settings.builder()
            .put("index.refresh_interval", "-1")
            .put("index.number_of_replicas", 2)
            .build();
        assertThat(request.settings(), equalTo(settings));

        assertThat(request.mappings(), containsString("field"));

        Alias alias = new Alias("my-alias");
        assertThat(request.aliases().size(), equalTo(1));
        assertThat(request.aliases().iterator().next(), equalTo(alias));
    }

    public void testSourceValidation() throws IOException {
        XContentBuilder indexPatterns = XContentFactory.jsonBuilder().startObject()
            .startObject("index_patterns")
                .field("my-pattern", "index-*")
            .endObject()
        .endObject();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new PutIndexTemplateRequest().source(indexPatterns));
        assertThat(e.getCause().getMessage(), containsString("Malformed [index_patterns] value"));

        XContentBuilder version = XContentFactory.jsonBuilder().startObject()
            .field("version", "v6.5.4")
        .endObject();
        e = expectThrows(IllegalArgumentException.class,() -> new PutIndexTemplateRequest().source(version));
        assertThat(e.getCause().getMessage(), containsString("Malformed [version] value"));

        XContentBuilder settings = XContentFactory.jsonBuilder().startObject()
            .field("settings", "index.number_of_replicas")
        .endObject();
        e = expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest().source(settings));
        assertThat(e.getCause().getMessage(), containsString("Malformed [settings] section"));

        XContentBuilder mappings = XContentFactory.jsonBuilder().startObject()
            .startObject("mappings")
                .field("_doc", "value")
            .endObject()
        .endObject();
        e = expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest().source(mappings));
        assertThat(e.getCause().getMessage(), containsString("Malformed [mappings] section"));

        XContentBuilder extraField = XContentFactory.jsonBuilder().startObject()
            .startObject("settings")
                .field("index.number_of_replicas", 2)
            .endObject()
            .field("extra-field", "value")
        .endObject();
        e = expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest().source(extraField));
        assertThat(e.getCause().getMessage(), containsString("unknown key [extra-field] in the template"));
    }
}
