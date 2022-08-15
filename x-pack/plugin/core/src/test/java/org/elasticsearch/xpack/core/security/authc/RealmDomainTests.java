/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RealmDomainTests extends AbstractSerializingTestCase<RealmDomain> {

    @Override
    protected RealmDomain doParseInstance(XContentParser parser) throws IOException {
        return RealmDomain.REALM_DOMAIN_PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<RealmDomain> instanceReader() {
        return RealmDomain::readFrom;
    }

    @Override
    protected RealmDomain createTestInstance() {
        return AuthenticationTestHelper.randomDomain(randomBoolean());
    }

}
