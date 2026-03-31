/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.service;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Objects;

public interface ServiceAccount {

    ServiceAccountId id();

    RoleDescriptor roleDescriptor();

    User asUser();

    final class ServiceAccountId {

        private final String namespace;
        private final String serviceName;

        public static ServiceAccountId fromPrincipal(String principal) {
            final int split = principal.indexOf('/');
            if (split == -1) {
                throw new IllegalArgumentException(
                    "a service account ID must be in the form {namespace}/{service-name}, but was [" + principal + "]"
                );
            }
            return new ServiceAccountId(principal.substring(0, split), principal.substring(split + 1));
        }

        public ServiceAccountId(String namespace, String serviceName) {
            this.namespace = namespace;
            this.serviceName = serviceName;
            if (Strings.isBlank(this.namespace)) {
                throw new IllegalArgumentException("the namespace of a service account ID must not be empty");
            }
            if (Strings.isBlank(this.serviceName)) {
                throw new IllegalArgumentException("the service-name of a service account ID must not be empty");
            }
        }

        public ServiceAccountId(StreamInput in) throws IOException {
            this.namespace = in.readString();
            this.serviceName = in.readString();
        }

        public void write(StreamOutput out) throws IOException {
            out.writeString(namespace);
            out.writeString(serviceName);
        }

        public String namespace() {
            return namespace;
        }

        public String serviceName() {
            return serviceName;
        }

        public String asPrincipal() {
            return namespace + "/" + serviceName;
        }

        @Override
        public String toString() {
            return asPrincipal();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServiceAccountId that = (ServiceAccountId) o;
            return namespace.equals(that.namespace) && serviceName.equals(that.serviceName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, serviceName);
        }
    }
}
