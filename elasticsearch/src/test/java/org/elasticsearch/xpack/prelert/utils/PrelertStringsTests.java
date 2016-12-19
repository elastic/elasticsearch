/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        assertEquals("\"ba\\\"r\\\"\"", PrelertStrings.doubleQuoteIfNotAlphaNumeric("ba\"r\""));
    }

    public void testIsValidId() {
        assertThat(PrelertStrings.isValidId("1_-.a"), is(true));
        assertThat(PrelertStrings.isValidId("b.-_3"), is(true));
        assertThat(PrelertStrings.isValidId("a-b.c_d"), is(true));

        assertThat(PrelertStrings.isValidId("a1_-."), is(false));
        assertThat(PrelertStrings.isValidId("-.a1_"), is(false));
        assertThat(PrelertStrings.isValidId(".a1_-"), is(false));
        assertThat(PrelertStrings.isValidId("_-.a1"), is(false));
        assertThat(PrelertStrings.isValidId("A"), is(false));
        assertThat(PrelertStrings.isValidId("!afafd"), is(false));
    }
}
