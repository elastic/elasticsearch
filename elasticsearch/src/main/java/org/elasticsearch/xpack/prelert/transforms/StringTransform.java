/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
            List<TransformIndex> readIndexes, List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);
        this.convertFunction = convertFunction;
        if (readIndexes.size() != 1 || writeIndexes.size() != 1) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public TransformResult transform(String[][] readWriteArea) throws TransformException {
        TransformIndex readIndex = readIndexes.get(0);
        TransformIndex writeIndex = writeIndexes.get(0);
        String input = readWriteArea[readIndex.array][readIndex.index];
        readWriteArea[writeIndex.array][writeIndex.index] = convertFunction.apply(input);
        return TransformResult.OK;
    }

    public static StringTransform createLowerCase(List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        return new StringTransform(s -> s.toLowerCase(Locale.ROOT), readIndexes, writeIndexes, logger);
    }

    public static StringTransform createUpperCase(List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        return new StringTransform(s -> s.toUpperCase(Locale.ROOT), readIndexes, writeIndexes, logger);
    }

    public static StringTransform createTrim(List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        return new StringTransform(s -> s.trim(), readIndexes, writeIndexes, logger);
    }
}
