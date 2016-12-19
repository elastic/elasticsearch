/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    /**
     * Immutable class for the domain split results
     */
    public static class DomainSplit {
        private String subDomain;
        private String highestRegisteredDomain;

        private DomainSplit(String subDomain, String highestRegisteredDomain) {
            this.subDomain = subDomain;
            this.highestRegisteredDomain = highestRegisteredDomain;
        }

        public String getSubDomain() {
            return subDomain;
        }

        public String getHighestRegisteredDomain() {
            return highestRegisteredDomain;
        }
    }

    public HighestRegisteredDomain(List<TransformIndex> readIndexes, List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);
    }

    @Override
    public TransformResult transform(String[][] readWriteArea) {
        return TransformResult.FAIL;
    }
}
