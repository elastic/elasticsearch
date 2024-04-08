/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionServiceSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenAiServiceSettingsTests extends AbstractWireSerializingTestCase<OpenAiServiceSettings> {

    public void testFromMap_CreatesSettingsCorrectly() {
        var modelId = "some model";
        var url = "https://www.elastic.co";
        var org = "organization";

        var serviceSettings = OpenAiServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId, ServiceFields.URL, url, OpenAiServiceFields.ORGANIZATION, org))
        );

        assertThat(serviceSettings, is(new OpenAiServiceSettings(modelId, ServiceUtils.createUri(url), org)));
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var modelId = "some model";
        var organization = "org";

        var serviceSettings = OpenAiServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId, OpenAiServiceFields.ORGANIZATION, organization))
        );

        assertNull(serviceSettings.uri());
        assertThat(serviceSettings.modelId(), is(modelId));
        assertThat(serviceSettings.organizationId(), is(organization));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "", ServiceFields.MODEL_ID, "model")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_MissingOrganization_DoesNotThrowException() {
        var modelId = "some model";

        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)));

        assertNull(serviceSettings.uri());
        assertThat(serviceSettings.modelId(), is(modelId));
    }

    public void testFromMap_EmptyOrganization_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiServiceSettings.fromMap(
                new HashMap<>(Map.of(OpenAiServiceFields.ORGANIZATION, "", ServiceFields.MODEL_ID, "model"))
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                org.elasticsearch.common.Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    OpenAiServiceFields.ORGANIZATION
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url, ServiceFields.MODEL_ID, "model")))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s];", url, ServiceFields.URL))
        );
    }

    @Override
    protected Writeable.Reader<OpenAiServiceSettings> instanceReader() {
        return OpenAiServiceSettings::new;
    }

    @Override
    protected OpenAiServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected OpenAiServiceSettings mutateInstance(OpenAiServiceSettings instance) throws IOException {
        return createRandomWithNonNullUrl();
    }

    public static OpenAiServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(8));
    }

    public static OpenAiServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);
        var organizationId = randomFrom(randomAlphaOfLength(15), null);

        return new OpenAiServiceSettings(modelId, ServiceUtils.createUri(url), organizationId);
    }
}
