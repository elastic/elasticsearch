/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws.imds;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class Ec2ImdsServiceBuilder {

    private final Ec2ImdsVersion ec2ImdsVersion;
    private BiConsumer<String, String> newCredentialsConsumer = Ec2ImdsServiceBuilder::rejectNewCredentials;
    private Collection<String> alternativeCredentialsEndpoints = Set.of();
    private Supplier<String> availabilityZoneSupplier = Ec2ImdsServiceBuilder::rejectAvailabilityZone;
    private ToXContent instanceIdentityDocument = null;
    private final Map<String, String> instanceAddresses = new HashMap<>();

    public Ec2ImdsServiceBuilder(Ec2ImdsVersion ec2ImdsVersion) {
        this.ec2ImdsVersion = ec2ImdsVersion;
    }

    public Ec2ImdsServiceBuilder newCredentialsConsumer(BiConsumer<String, String> newCredentialsConsumer) {
        this.newCredentialsConsumer = newCredentialsConsumer;
        return this;
    }

    private static void rejectNewCredentials(String ignored1, String ignored2) {
        ESTestCase.fail("credentials creation not supported");
    }

    public Ec2ImdsServiceBuilder alternativeCredentialsEndpoints(Collection<String> alternativeCredentialsEndpoints) {
        this.alternativeCredentialsEndpoints = alternativeCredentialsEndpoints;
        return this;
    }

    private static String rejectAvailabilityZone() {
        return ESTestCase.fail(null, "availability zones not supported");
    }

    public Ec2ImdsServiceBuilder availabilityZoneSupplier(Supplier<String> availabilityZoneSupplier) {
        this.availabilityZoneSupplier = availabilityZoneSupplier;
        return this;
    }

    public Ec2ImdsServiceBuilder addInstanceAddress(String addressType, String addressValue) {
        instanceAddresses.put("/latest/meta-data/" + addressType, addressValue);
        return this;
    }

    public Ec2ImdsHttpHandler buildHandler() {
        return new Ec2ImdsHttpHandler(
            ec2ImdsVersion,
            newCredentialsConsumer,
            alternativeCredentialsEndpoints,
            availabilityZoneSupplier,
            instanceIdentityDocument,
            Map.copyOf(instanceAddresses)
        );
    }

    public Ec2ImdsServiceBuilder instanceIdentityDocument(ToXContent instanceIdentityDocument) {
        this.instanceIdentityDocument = instanceIdentityDocument;
        return this;
    }
}
