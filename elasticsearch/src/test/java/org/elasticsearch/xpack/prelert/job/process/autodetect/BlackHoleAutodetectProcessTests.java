/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    public void testFlushJob_writesAck() throws Exception {
        try (BlackHoleAutodetectProcess process = new BlackHoleAutodetectProcess()) {

            String flushId = process.flushJob(InterimResultsParams.builder().build());

            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(process.getProcessOutStream());
            parser.nextToken(); // FlushAcknowledgementParser expects this to be
                                // called first
            AutodetectResult result = AutodetectResult.PARSER.apply(parser, () -> ParseFieldMatcher.STRICT);
            FlushAcknowledgement ack = result.getFlushAcknowledgement();
            assertEquals(flushId, ack.getId());
        }
    }
}