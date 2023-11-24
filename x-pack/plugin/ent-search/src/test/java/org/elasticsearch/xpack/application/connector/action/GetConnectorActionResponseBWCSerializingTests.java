/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

public class GetConnectorActionResponseBWCSerializingTests {

    // private BytesReference connector;
    //
    // @Override
    // protected Writeable.Reader<GetConnectorAction.Response> instanceReader() {
    // return GetConnectorAction.Response::new;
    // }
    //
    // @Override
    // protected GetConnectorAction.Response createTestInstance() {
    // this.connector = ConnectorTestUtils.getRandomConnector();
    // }
    //
    // @Override
    // protected GetConnectorAction.Response mutateInstance(GetConnectorAction.Response instance) throws IOException {
    // return randomValueOtherThan(instance, this::createTestInstance);
    // }
    //
    // @Override
    // protected GetConnectorAction.Response doParseInstance(XContentParser parser) throws IOException {
    // return null;
    // }
    //
    // @Override
    // protected GetConnectorAction.Response mutateInstanceForVersion(GetConnectorAction.Response instance, TransportVersion version) {
    // return null;
    // }
}
