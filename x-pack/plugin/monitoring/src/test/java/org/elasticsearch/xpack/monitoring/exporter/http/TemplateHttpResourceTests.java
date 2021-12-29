/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests {@link TemplateHttpResource}.
 */
public class TemplateHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String templateName = ".my_template";

    // the internal representation has the type, the external representation should not
    private final String templateValueInternal = """
        {
          "order": 0,
          "index_patterns": [ ".xyz-*" ],
          "settings": {},
          "mappings": {
            "_doc": {
              "properties": {
                "one": {
                  "properties": {
                    "two": {
                      "properties": {
                        "name": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }
          },
          "aliases": {}
        }""";
    private final String templateValueExternal = """
        {
          "order": 0,
          "index_patterns": [ ".xyz-*" ],
          "settings": {},
          "mappings": {
            "properties": {
              "one": {
                "properties": {
                  "two": {
                    "properties": {
                      "name": {
                        "type": "keyword"
                      }
                    }
                  }
                }
              }
            }
          },
          "aliases": {}
        }""";
    private final Supplier<String> template = () -> templateValueInternal;
    private final int minimumVersion = Math.min(MonitoringTemplateUtils.LAST_UPDATED_VERSION, Version.CURRENT.id);

    private final TemplateHttpResource resource = new TemplateHttpResource(owner, masterTimeout, templateName);

    public void testDoCheckExists() {
        final HttpEntity entity = entityForResource(true, templateName, minimumVersion);

        doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), true, entity);
    }

    public void testDoCheckDoesNotExist() {
        if (randomBoolean()) {
            // it does not exist because it's literally not there
            assertCheckDoesNotExist(resource, "/_template", templateName);
        } else {
            // it does not exist because we need to replace it
            final HttpEntity entity = entityForResource(false, templateName, minimumVersion);

            doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), false, entity);
        }
    }

    public void testDoCheckError() {
        if (randomBoolean()) {
            // error because of a server error
            assertCheckWithException(resource, "/_template", templateName);
        } else {
            // error because of a malformed response
            final HttpEntity entity = entityForResource(null, templateName, minimumVersion);

            doCheckWithStatusCode(resource, "/_template", templateName, successfulCheckStatus(), null, entity);
        }
    }

    public void testDoPublishFalseWithNonPublishedResource() {
        RestClient mockClient = mock(RestClient.class);
        SetOnce<HttpResource.ResourcePublishResult> result = new SetOnce<>();
        resource.doPublish(mockClient, ActionListener.wrap(result::set, e -> { throw new RuntimeException("Unexpected exception", e); }));
        verifyNoMoreInteractions(mockClient); // Should not have used the client at all.
        HttpResource.ResourcePublishResult resourcePublishResult = result.get();
        assertThat(resourcePublishResult, notNullValue());
        assertThat(resourcePublishResult.getResourceState(), notNullValue());
        assertThat(resourcePublishResult.getResourceState(), is(HttpResource.State.DIRTY));
        assertThat(
            resourcePublishResult.getReason(),
            is("waiting for remote monitoring cluster to install appropriate template [.my_template] (version mismatch or missing)")
        );
    }

    public void testParameters() {
        assertVersionParameters(resource);
    }

}
