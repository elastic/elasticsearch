/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponseTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.synonyms.SynonymsManagementAPIService.SynonymsReloadResult;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.synonyms.SynonymUpdateResponse.EMPTY_RELOAD_ANALYZER_RESPONSE;
import static org.elasticsearch.synonyms.SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SynonymUpdateResponseSerializingTests extends AbstractBWCSerializationTestCase<SynonymUpdateResponse> {

    private static final ConstructingObjectParser<SynonymUpdateResponse, Void> PARSER = new ConstructingObjectParser<>(
        "synonyms_update_response",
        true,
        arg -> {
            SynonymsManagementAPIService.UpdateSynonymsResultStatus status = SynonymsManagementAPIService.UpdateSynonymsResultStatus
                .valueOf(((String) arg[0]).toUpperCase(Locale.ROOT));
            ReloadAnalyzersResponse reloadAnalyzersResponse = (ReloadAnalyzersResponse) arg[1];
            return new SynonymUpdateResponse(new SynonymsReloadResult(status, reloadAnalyzersResponse));
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(SynonymUpdateResponse.RESULT_FIELD));
        PARSER.declareObjectOrNull(
            optionalConstructorArg(),
            (p, c) -> ReloadAnalyzersResponseTests.PARSER.parse(p, null),
            null,
            new ParseField(SynonymUpdateResponse.RELOAD_ANALYZERS_DETAILS_FIELD)
        );
    }

    private static final TransportVersion SYNONYMS_REFRESH_PARAM = TransportVersion.fromName("synonyms_refresh_param");

    @Override
    protected Writeable.Reader<SynonymUpdateResponse> instanceReader() {
        return SynonymUpdateResponse::new;
    }

    @Override
    protected SynonymUpdateResponse createTestInstance() {
        return createTestInstance(randomBoolean());
    }

    private static SynonymUpdateResponse createTestInstance(boolean includeReloadInfo) {
        return new SynonymUpdateResponse(
            new SynonymsReloadResult(
                randomFrom(SynonymsManagementAPIService.UpdateSynonymsResultStatus.values()),
                randomReloadAnalyzersResponse(includeReloadInfo)
            )
        );
    }

    private static ReloadAnalyzersResponse randomReloadAnalyzersResponse(boolean includeReloadInfo) {
        ReloadAnalyzersResponse reloadAnalyzersResponse = null;
        if (includeReloadInfo) {
            Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesDetails = ReloadAnalyzersResponseTests
                .createRandomReloadDetails();
            reloadAnalyzersResponse = new ReloadAnalyzersResponse(
                randomIntBetween(0, 10),
                randomIntBetween(0, 10),
                randomIntBetween(0, 5),
                null,
                reloadedIndicesDetails
            );
        }
        return reloadAnalyzersResponse;
    }

    @Override
    protected SynonymUpdateResponse mutateInstance(SynonymUpdateResponse instance) throws IOException {
        SynonymsManagementAPIService.UpdateSynonymsResultStatus updateStatus = instance.updateStatus();
        ReloadAnalyzersResponse reloadAnalyzersResponse = instance.reloadAnalyzersResponse();
        switch (between(0, 1)) {
            case 0 -> updateStatus = randomValueOtherThan(
                updateStatus,
                () -> randomFrom(SynonymsManagementAPIService.UpdateSynonymsResultStatus.values())
            );
            case 1 -> reloadAnalyzersResponse = randomValueOtherThan(
                reloadAnalyzersResponse,
                () -> randomReloadAnalyzersResponse(randomBoolean())
            );
        }
        return new SynonymUpdateResponse(new SynonymsReloadResult(updateStatus, reloadAnalyzersResponse));
    }

    @Override
    protected SynonymUpdateResponse mutateInstanceForVersion(SynonymUpdateResponse instance, TransportVersion version) {

        if (version.supports(SYNONYMS_REFRESH_PARAM) == false && instance.reloadAnalyzersResponse() == null) {
            // Nulls will be written as empty reload analyzer responses for older versions
            return new SynonymUpdateResponse(new SynonymsReloadResult(instance.updateStatus(), EMPTY_RELOAD_ANALYZER_RESPONSE));
        }

        return instance;
    }

    public void testToXContent() throws IOException {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesNodes = Collections.singletonMap(
            "index",
            new ReloadAnalyzersResponse.ReloadDetails("index", Collections.singleton("nodeId"), Collections.singleton("my_analyzer"))
        );
        ReloadAnalyzersResponse reloadAnalyzersResponse = new ReloadAnalyzersResponse(10, 5, 0, null, reloadedIndicesNodes);
        SynonymUpdateResponse response = new SynonymUpdateResponse(new SynonymsReloadResult(CREATED, reloadAnalyzersResponse));

        String output = Strings.toString(response);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "result": "created",
              "reload_analyzers_details": {
                "_shards": {
                  "total": 10,
                  "successful": 5,
                  "failed": 0
                },
                "reload_details": [
                  {
                    "index": "index",
                    "reloaded_analyzers": [ "my_analyzer" ],
                    "reloaded_node_ids": [ "nodeId" ]
                  }
                ]
              }
            }"""), output);
    }

    public void testToXContentWithNoReloadResult() throws IOException {
        SynonymUpdateResponse response = new SynonymUpdateResponse(new SynonymsReloadResult(CREATED, null));
        String output = Strings.toString(response);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "result": "created"
            }"""), output);
    }

    @Override
    protected SynonymUpdateResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }
}
