/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class OpenShiftAiEmbeddingsRequestEntityTests extends ESTestCase {

    private static final String MODEL = "some model";
    private static final String INPUT = "some input";

    public void testXContent_DoesNotWriteDimensionsWhenNullAndSetByUserIsFalse() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(List.of(INPUT), MODEL, null, false);
        testXContent_DoesNotWriteDimensions(entity);
    }

    public void testXContent_DoesNotWriteDimensionsWhenNotSetByUser() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(List.of(INPUT), MODEL, 100, false);
        testXContent_DoesNotWriteDimensions(entity);
    }

    public void testXContent_DoesNotWriteDimensionsWhenNull_EvenIfSetByUserIsTrue() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(List.of(INPUT), MODEL, null, true);
        testXContent_DoesNotWriteDimensions(entity);
    }

    private static void testXContent_DoesNotWriteDimensions(OpenShiftAiEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"],
                "model": "some model"
            }
            """)));
    }

    public void testXContent_DoesNotWriteModelWhenItIsNull() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(List.of(INPUT), null, null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"]
            }
            """)));
    }

    public void testXContent_WritesDimensionsWhenNonNull_AndSetByUserIsTrue() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(List.of(INPUT), MODEL, 100, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"],
                "model": "some model",
                "dimensions": 100
            }
            """)));
    }
}
