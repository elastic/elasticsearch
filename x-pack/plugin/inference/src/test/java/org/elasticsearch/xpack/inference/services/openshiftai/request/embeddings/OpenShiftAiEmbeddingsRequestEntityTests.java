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

    private static final String MODEL_VALUE = "some_model";
    private static final List<String> INPUT_VALUE = List.of("some input");
    private static final int DIMENSIONS_VALUE = 100;

    public void testXContent_DoesNotWriteDimensionsWhenNullAndSetByUserIsFalse() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(INPUT_VALUE, MODEL_VALUE, null, false);
        testXContent_DoesNotWriteDimensions(entity);
    }

    public void testXContent_DoesNotWriteDimensionsWhenNotSetByUser() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(INPUT_VALUE, MODEL_VALUE, DIMENSIONS_VALUE, false);
        testXContent_DoesNotWriteDimensions(entity);
    }

    public void testXContent_DoesNotWriteDimensionsWhenNull_EvenIfSetByUserIsTrue() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(INPUT_VALUE, MODEL_VALUE, null, true);
        testXContent_DoesNotWriteDimensions(entity);
    }

    private static void testXContent_DoesNotWriteDimensions(OpenShiftAiEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"],
                "model": "some_model"
            }
            """)));
    }

    public void testXContent_DoesNotWriteModelWhenItIsNull() throws IOException {
        var entity = new OpenShiftAiEmbeddingsRequestEntity(INPUT_VALUE, null, null, false);

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
        var entity = new OpenShiftAiEmbeddingsRequestEntity(INPUT_VALUE, MODEL_VALUE, DIMENSIONS_VALUE, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"],
                "model": "some_model",
                "dimensions": 100
            }
            """)));
    }
}
