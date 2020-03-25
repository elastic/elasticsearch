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

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.license.StartTrialRequest;
import org.elasticsearch.client.license.StartTrialResponse;
import org.elasticsearch.client.license.StartBasicRequest;
import org.elasticsearch.client.license.StartBasicResponse;
import org.elasticsearch.client.license.GetBasicStatusResponse;
import org.elasticsearch.client.license.GetTrialStatusResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.license.DeleteLicenseRequest;
import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.GetLicenseResponse;
import org.elasticsearch.client.license.PutLicenseRequest;
import org.elasticsearch.client.license.PutLicenseResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic License-related methods
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/licensing-apis.html">
 * X-Pack Licensing APIs on elastic.co</a> for more information.
 */
public final class LicenseClient {

    private final RestHighLevelClient restHighLevelClient;

    LicenseClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Updates license for the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutLicenseResponse putLicense(PutLicenseRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, LicenseRequestConverters::putLicense, options,
            PutLicenseResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously updates license for the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putLicenseAsync(PutLicenseRequest request, RequestOptions options, ActionListener<PutLicenseResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, LicenseRequestConverters::putLicense, options,
            PutLicenseResponse::fromXContent, listener, emptySet());
    }

    /**
     * Returns the current license for the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetLicenseResponse getLicense(GetLicenseRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(request, LicenseRequestConverters::getLicense, options,
            response -> new GetLicenseResponse(convertResponseToJson(response)), emptySet());
    }

    /**
     * Asynchronously returns the current license for the cluster cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getLicenseAsync(GetLicenseRequest request, RequestOptions options, ActionListener<GetLicenseResponse> listener) {
        return restHighLevelClient.performRequestAsync(request, LicenseRequestConverters::getLicense, options,
            response -> new GetLicenseResponse(convertResponseToJson(response)), listener, emptySet());
    }

    /**
     * Deletes license from the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteLicense(DeleteLicenseRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, LicenseRequestConverters::deleteLicense, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously deletes license from the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteLicenseAsync(DeleteLicenseRequest request, RequestOptions options,
                                          ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            LicenseRequestConverters::deleteLicense, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Starts a trial license on the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public StartTrialResponse startTrial(StartTrialRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, LicenseRequestConverters::startTrial, options,
            StartTrialResponse::fromXContent, singleton(403));
    }

    /**
     * Asynchronously starts a trial license on the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startTrialAsync(StartTrialRequest request,
                                       RequestOptions options,
                                       ActionListener<StartTrialResponse> listener) {

        return restHighLevelClient.performRequestAsyncAndParseEntity(request, LicenseRequestConverters::startTrial, options,
            StartTrialResponse::fromXContent, listener, singleton(403));
    }

    /**
     * Initiates an indefinite basic license.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public StartBasicResponse startBasic(StartBasicRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, LicenseRequestConverters::startBasic, options,
            StartBasicResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously initiates an indefinite basic license.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable startBasicAsync(StartBasicRequest request, RequestOptions options,
                                       ActionListener<StartBasicResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, LicenseRequestConverters::startBasic, options,
            StartBasicResponse::fromXContent, listener, emptySet());
    }

    /**
     * Retrieve the license trial status
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetTrialStatusResponse getTrialStatus(RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(Validatable.EMPTY,
            request -> LicenseRequestConverters.getLicenseTrialStatus(), options, GetTrialStatusResponse::fromXContent, emptySet());
    }

    /**
     * Retrieve the license basic status
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetBasicStatusResponse getBasicStatus(RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(Validatable.EMPTY,
            request -> LicenseRequestConverters.getLicenseBasicStatus(), options, GetBasicStatusResponse::fromXContent, emptySet());
    }

    /**
     * Converts an entire response into a json string
     *
     * This is useful for responses that we don't parse on the client side, but instead work as string
     * such as in case of the license JSON
     */
    static String convertResponseToJson(Response response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        if (xContentType == XContentType.JSON) {
            // No changes is required
            return Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
        } else {
            // Need to convert into JSON
            try (InputStream stream = response.getEntity().getContent();
                 XContentParser parser = XContentFactory.xContent(xContentType).createParser(NamedXContentRegistry.EMPTY,
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
                parser.nextToken();
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.copyCurrentStructure(parser);
                return Strings.toString(builder);
            }
        }
    }
}
