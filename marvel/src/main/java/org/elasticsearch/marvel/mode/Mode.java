/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.mode;

import org.elasticsearch.ElasticsearchException;

import java.util.Locale;

/**
 * Marvel's operating mode
 */
public enum Mode {

    /**
     * Marvel runs in downgraded mode
     */
    TRIAL(0),

    /**
     * Marvel runs in downgraded mode
     */
    LITE(0),

    /**
     * Marvel runs in normal mode
     */
    STANDARD(1);

    private final byte id;

    Mode(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static Mode fromId(byte id) {
        switch (id) {
            case 0:
                return TRIAL;
            case 1:
                return LITE;
            case 2:
                return STANDARD;
            case 3:
            default:
                throw new ElasticsearchException("unknown marvel mode id [" + id + "]");
        }
    }

    public static Mode fromName(String name) {
        switch (name.toLowerCase(Locale.ROOT)) {
            case "trial": return TRIAL;
            case "lite": return LITE;
            case "standard" : return STANDARD;
            default:
                throw new ElasticsearchException("unknown marvel mode name [" + name + "]");
        }
    }
}
