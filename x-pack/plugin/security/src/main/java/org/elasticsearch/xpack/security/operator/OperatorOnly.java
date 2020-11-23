/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.transport.TransportRequest;

import java.util.function.Supplier;

public interface OperatorOnly {

    Result check(String action, TransportRequest request);

    enum Status {
        YES, NO, CONTINUE;
    }

    final class Result {
        private final Status status;
        private final Supplier<String> messageSupplier;

        private Result(Status status, Supplier<String> messageSupplier) {
            this.status = status;
            this.messageSupplier = messageSupplier;
        }

        static Result yes(Supplier<String> messageSupplier) {
            return new Result(Status.YES, messageSupplier);
        }

        public Status getStatus() {
            return status;
        }

        public String getMessage() {
            return messageSupplier.get();
        }
    }

    Result RESULT_NO = new Result(Status.NO, null);
    Result RESULT_CONTINUE = new Result(Status.CONTINUE, null);
}
