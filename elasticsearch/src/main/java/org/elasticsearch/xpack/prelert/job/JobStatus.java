/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
public enum JobStatus implements Writeable {

    CLOSING, CLOSED, OPENING, OPENED, FAILED;

    public static JobStatus fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static JobStatus fromStream(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IOException("Unknown public enum JobStatus {\n ordinal [" + ordinal + "]");
        }
        return values()[ordinal];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(ordinal());
    }

    /**
     * @return {@code true} if status matches any of the given {@code candidates}
     */
    public boolean isAnyOf(JobStatus... candidates) {
        return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
    }
}
