/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
     *                  strings and will never contain a <code>null</code>
     * @param gotFields boolean array each element is true if that field
     *                  was read
     * @return The number of fields in the JSON doc or -1 if nothing was read
     * because the end of the stream was reached
     */
    long read(String[] record, boolean[] gotFields) throws IOException;
}
