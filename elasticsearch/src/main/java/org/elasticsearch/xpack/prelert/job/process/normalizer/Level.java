/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    INFLUENCER("infl"),
    PARTITION("part");

    private final String m_Key;

    Level(String key) {
        m_Key = key;
    }

    public String asString() {
        return m_Key;
    }
}
