/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import java.util.Objects;

/**
 * Represents the action field of a BYO (bring-your-own) semantic_text value.
 * Users supply pre-computed chunks and vectors by specifying one of these
 * actions in the {@code _action} field of the document's semantic_text value.
 * The actions correspond to phases of a multi-part upload protocol:
 * <ul>
 *   <li>{@link #STAGE_INIT} — initialise a new staged upload session</li>
 *   <li>{@link #STAGE} — upload a chunk of vectors into an active session</li>
 *   <li>{@link #COMMIT} — finalise the session and make the data searchable</li>
 *   <li>{@link #CANCEL} — abandon the session and discard staged data</li>
 * </ul>
 */
public enum BYOSemanticAction {

    STAGE_INIT("stage_init"),
    STAGE("stage"),
    COMMIT("commit"),
    CANCEL("cancel");

    private final String value;

    BYOSemanticAction(String value) {
        this.value = value;
    }

    /**
     * Returns the JSON key name used to convey the action in a document.
     */
    public static String actionField() {
        return "_action";
    }

    /**
     * Parses the given string to a {@link BYOSemanticAction}, ignoring case.
     *
     * @param value the string to parse; must not be {@code null}
     * @return the matching action
     * @throws NullPointerException     if {@code value} is {@code null}
     * @throws IllegalArgumentException if {@code value} does not match any action
     */
    public static BYOSemanticAction fromString(String value) {
        Objects.requireNonNull(value);
        for (BYOSemanticAction action : values()) {
            if (action.value.equalsIgnoreCase(value)) {
                return action;
            }
        }
        throw new IllegalArgumentException("Unknown BYO semantic action [" + value + "]");
    }
}
