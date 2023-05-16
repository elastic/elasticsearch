/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.synonyms.SynonymSet;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class PutSynonymsAction extends ActionType<AcknowledgedResponse> {

    public static final PutSynonymsAction INSTANCE = new PutSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/put";

    public PutSynonymsAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest {
        private final String name;
        private final SynonymSet synonymSet;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.synonymSet = new SynonymSet(in);
        }

        public Request(String name, BytesReference content, XContentType contentType) throws IOException {
            this.name = name;
            this.synonymSet = SynonymSet.fromXContent(XContentHelper.createParser(XContentParserConfiguration.EMPTY, content, contentType));
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isEmpty(name)) {
                validationException = ValidateActions.addValidationError("synonym set must be specified", validationException);
            }

            // TODO Synonym validation - use current synonyms parser?
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            synonymSet.writeTo(out);
        }

        public String name() {
            return name;
        }

        public SynonymSet synonymSet() {
            return synonymSet;
        }
    }
}
