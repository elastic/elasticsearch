/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
     * code.
     */
    String CONTROL_FIELD_NAME = ".";

    /**
     * Write each String in the record array
     */
    void writeRecord(String[] record) throws IOException;

    /**
     * Write each String in the record list
     */
    void writeRecord(List<String> record) throws IOException;

    /**
     * Flush the outputIndex stream.
     */
    void flush() throws IOException;

}