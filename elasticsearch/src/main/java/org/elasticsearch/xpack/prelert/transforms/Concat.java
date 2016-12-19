/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    private static final String EMPTY_STRING = "";

    private final String delimiter;

    public Concat(List<TransformIndex> readIndexes, List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);
        delimiter = EMPTY_STRING;
    }

    public Concat(String join, List<TransformIndex> readIndexes, List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);
        delimiter = join;
    }

    public String getDelimiter() {
        return delimiter;
    }

    /**
     * Concat has only 1 output field
     */
    @Override
    public TransformResult transform(String[][] readWriteArea)
            throws TransformException {
        if (writeIndexes.isEmpty()) {
            return TransformResult.FAIL;
        }

        TransformIndex writeIndex = writeIndexes.get(0);

        StringJoiner joiner = new StringJoiner(delimiter);
        for (TransformIndex i : readIndexes) {
            joiner.add(readWriteArea[i.array][i.index]);
        }
        readWriteArea[writeIndex.array][writeIndex.index] = joiner.toString();

        return TransformResult.OK;
    }
}
