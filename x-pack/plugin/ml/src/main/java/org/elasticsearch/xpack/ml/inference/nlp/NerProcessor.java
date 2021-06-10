/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Locale;

public class NerProcessor implements NlpTask.Processor {

    public enum Entity implements Writeable {
        NONE, MISC, PERSON, ORGANISATION, LOCATION;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // Inside-Outside-Beginning (IOB) tag
    enum IobTag {
        O(Entity.NONE),                 // Outside of a named entity
        B_MISC(Entity.MISC),            // Beginning of a miscellaneous entity right after another miscellaneous entity
        I_MISC(Entity.MISC),            // Miscellaneous entity
        B_PER(Entity.PERSON),           // Beginning of a person's name right after another person's name
        I_PER(Entity.PERSON),           // Person's name
        B_ORG(Entity.ORGANISATION),     // Beginning of an organisation right after another organisation
        I_ORG(Entity.ORGANISATION),     // Organisation
        B_LOC(Entity.LOCATION),         // Beginning of a location right after another location
        I_LOC(Entity.LOCATION);         // Location

        private final Entity entity;

        IobTag(Entity entity) {
            this.entity = entity;
        }

        Entity getEntity() {
            return entity;
        }

        boolean isBeginning() {
            return name().toLowerCase(Locale.ROOT).startsWith("b");
        }
    }


    private final BertRequestBuilder bertRequestBuilder;

    NerProcessor(BertTokenizer tokenizer) {
        this.bertRequestBuilder = new BertRequestBuilder(tokenizer);
    }

    @Override
    public void validateInputs(String inputs) {
        // No validation
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return bertRequestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor() {
        return new NerResultProcessor(bertRequestBuilder.getTokenization());
    }
}
