/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    private final Client client;

    public JobDataDeleterFactory(Client client) {
        this.client = client;
    }

    @Override
    public JobDataDeleter apply(String jobId) {
        return new JobDataDeleter(client, jobId);
    }
}
