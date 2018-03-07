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

package org.elasticsearch.repositories.gcs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.testing.http.MockHttpTransport;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageServiceTests extends ESTestCase {

    /**
     * Test that the {@link GoogleCloudStorageService.DefaultHttpRequestInitializer} attaches new instances
     * of {@link HttpIOExceptionHandler} and {@link HttpUnsuccessfulResponseHandler} for every HTTP requests.
     */
    public void testDefaultHttpRequestInitializer() throws IOException {
        final Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        final GoogleCredential credential = mock(GoogleCredential.class);
        when(credential.handleResponse(any(HttpRequest.class), any(HttpResponse.class), anyBoolean())).thenReturn(false);

        final TimeValue readTimeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        final TimeValue connectTimeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        final String endpoint = randomBoolean() ? randomAlphaOfLength(10) : null;
        final String applicationName = randomBoolean() ? randomAlphaOfLength(10) : null;

        final GoogleCloudStorageClientSettings clientSettings =
            new GoogleCloudStorageClientSettings(credential, endpoint, connectTimeout, readTimeout, applicationName);

        final HttpRequestInitializer initializer = GoogleCloudStorageService.createRequestInitializer(clientSettings);
        final HttpRequestFactory requestFactory = new MockHttpTransport().createRequestFactory(initializer);

        final HttpRequest request1 = requestFactory.buildGetRequest(new GenericUrl());
        assertEquals((int) connectTimeout.millis(), request1.getConnectTimeout());
        assertEquals((int) readTimeout.millis(), request1.getReadTimeout());
        assertSame(credential, request1.getInterceptor());
        assertNotNull(request1.getIOExceptionHandler());
        assertNotNull(request1.getUnsuccessfulResponseHandler());

        final HttpRequest request2 = requestFactory.buildGetRequest(new GenericUrl());
        assertEquals((int) connectTimeout.millis(), request2.getConnectTimeout());
        assertEquals((int) readTimeout.millis(), request2.getReadTimeout());
        assertSame(request1.getInterceptor(), request2.getInterceptor());
        assertNotNull(request2.getIOExceptionHandler());
        assertNotSame(request1.getIOExceptionHandler(), request2.getIOExceptionHandler());
        assertNotNull(request2.getUnsuccessfulResponseHandler());
        assertNotSame(request1.getUnsuccessfulResponseHandler(), request2.getUnsuccessfulResponseHandler());

        request1.getUnsuccessfulResponseHandler().handleResponse(null, null, false);
        verify(credential, times(1)).handleResponse(any(HttpRequest.class), any(HttpResponse.class), anyBoolean());

        request2.getUnsuccessfulResponseHandler().handleResponse(null, null, false);
        verify(credential, times(2)).handleResponse(any(HttpRequest.class), any(HttpResponse.class), anyBoolean());
    }

    public void testToTimeout() {
        assertNull(GoogleCloudStorageService.toTimeout(null));
        assertNull(GoogleCloudStorageService.toTimeout(TimeValue.ZERO));
        assertEquals(0, GoogleCloudStorageService.toTimeout(TimeValue.MINUS_ONE).intValue());
    }
}
