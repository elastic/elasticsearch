/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    @Override
    protected Influence createTestInstance() {
        int size = randomInt(10);
        List<String> fieldValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            fieldValues.add(randomAsciiOfLengthBetween(1, 20));
        }
        return new Influence(randomAsciiOfLengthBetween(1, 30), fieldValues);
    }

    @Override
    protected Reader<Influence> instanceReader() {
        return Influence::new;
    }

    @Override
    protected Influence parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Influence.PARSER.apply(parser, () -> matcher);
    }

}
