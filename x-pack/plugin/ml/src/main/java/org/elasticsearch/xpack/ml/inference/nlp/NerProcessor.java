/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
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
    private final IobTag[] iobMap;

    NerProcessor(BertTokenizer tokenizer, @Nullable List<String> classificationLabels) {
        this.bertRequestBuilder = new BertRequestBuilder(tokenizer);
        validate(classificationLabels);
        iobMap = buildIobMap(classificationLabels);
    }

    /**
     * Checks labels are valid entity tags and none are duplicated
     */
    private void validate(List<String> classificationLabels) {
        if (classificationLabels == null || classificationLabels.isEmpty()) {
            return;
        }

        ValidationException ve = new ValidationException();
        EnumSet<IobTag> tags = EnumSet.noneOf(IobTag.class);
        for (String label : classificationLabels) {
            try {
                IobTag iobTag = IobTag.valueOf(label);
                if (tags.contains(iobTag)) {
                    ve.addValidationError("the classification label [" + label + "] is duplicated in the list " + classificationLabels);
                }
                tags.add(iobTag);
            } catch (IllegalArgumentException iae) {
                ve.addValidationError("classification label [" + label + "] is not an entity I-O-B tag.");
            }
        }

        if (ve.validationErrors().isEmpty() == false) {
            ve.addValidationError("Valid entity I-O-B tags are " + Arrays.toString(IobTag.values()));
            throw ve;
        }
    }

    static IobTag[] buildIobMap(List<String> classificationLabels) {
        if (classificationLabels == null || classificationLabels.isEmpty()) {
            return IobTag.values();
        }

        IobTag[] map = new IobTag[classificationLabels.size()];
        for (int i=0; i<classificationLabels.size(); i++) {
            map[i] = IobTag.valueOf(classificationLabels.get(i));
        }

        return map;
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
        return new NerResultProcessor(bertRequestBuilder.getTokenization(), iobMap);
    }
}
