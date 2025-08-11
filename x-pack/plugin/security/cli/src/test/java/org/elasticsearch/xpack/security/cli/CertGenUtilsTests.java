/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for cert utils
 */
public class CertGenUtilsTests extends ESTestCase {

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
    }

    public void testSerialNotRepeated() {
        int iterations = scaledRandomIntBetween(10, 100);
        List<BigInteger> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++) {
            BigInteger serial = CertGenUtils.getSerial();
            assertThat(list.contains(serial), is(false));
            list.add(serial);
        }
    }

    public void testGenerateKeyPair() throws Exception {
        KeyPair keyPair = CertGenUtils.generateKeyPair(randomFrom(1024, 2048));
        assertThat(keyPair.getPrivate().getAlgorithm(), is("RSA"));
        assertThat(keyPair.getPublic().getAlgorithm(), is("RSA"));
    }

    public void testSubjectAlternativeNames() throws Exception {
        final boolean resolveName = randomBoolean();
        InetAddress address = InetAddresses.forString("127.0.0.1");

        GeneralNames generalNames = CertGenUtils.getSubjectAlternativeNames(resolveName, Collections.singleton(address));
        assertThat(generalNames, notNullValue());
        GeneralName[] generalNameArray = generalNames.getNames();
        assertThat(generalNameArray, notNullValue());

        logger.info("resolve name [{}], address [{}], subject alt names [{}]", resolveName, NetworkAddress.format(address), generalNames);
        if (resolveName && isResolvable(address)) {
            assertThat(generalNameArray.length, is(2));
            int firstType = generalNameArray[0].getTagNo();
            if (firstType == GeneralName.iPAddress) {
                assertThat(generalNameArray[1].getTagNo(), is(GeneralName.dNSName));
            } else if (firstType == GeneralName.dNSName) {
                assertThat(generalNameArray[1].getTagNo(), is(GeneralName.iPAddress));
            } else {
                fail("unknown tag value: " + firstType);
            }
        } else {
            assertThat(generalNameArray.length, is(1));
            assertThat(generalNameArray[0].getTagNo(), is(GeneralName.iPAddress));
        }
    }

    @SuppressForbidden(reason = "need to use getHostName to resolve DNS name and getHostAddress to ensure we resolved the name")
    private boolean isResolvable(InetAddress inetAddress) {
        String hostname = inetAddress.getHostName();
        return hostname.equals(inetAddress.getHostAddress()) == false;
    }

}
