/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.settings.SecureString;
import org.hamcrest.Matchers;
import org.opensaml.saml.saml2.core.NameID;

import java.util.List;

public class SamlAttributesTests extends SamlTestCase {

    public void testToString() {
        final String nameFormat = randomFrom(NameID.TRANSIENT, NameID.PERSISTENT, NameID.EMAIL);
        final String nameId = randomIdentifier();
        final String session = randomAlphaOfLength(16);
        final String inResponseTo = randomAlphanumericOfLength(16);
        final SamlAttributes attributes = new SamlAttributes(
            new SamlNameId(nameFormat, nameId, null, null, null),
            session,
            inResponseTo,
            List.of(
                new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.1", null, List.of("peter.ng")),
                new SamlAttributes.SamlAttribute("urn:oid:2.5.4.3", "name", List.of("Peter Ng")),
                new SamlAttributes.SamlAttribute(
                    "urn:oid:1.3.6.1.4.1.5923.1.5.1.1",
                    "groups",
                    List.of("employees", "engineering", "managers")
                )
            ),
            List.of(
                new SamlAttributes.SamlPrivateAttribute(
                    "urn:oid:0.9.2342.19200300.100.1.3",
                    "mail",
                    List.of(new SecureString("peter@ng.com".toCharArray()))
                )
            )
        );
        assertThat(
            attributes.toString(),
            Matchers.equalTo(
                "SamlAttributes("
                    + ("NameId(" + nameFormat + ")=" + nameId)
                    + ")["
                    + session
                    + "]["
                    + inResponseTo
                    + "]{["
                    + "urn:oid:0.9.2342.19200300.100.1.1=[peter.ng](len=1)"
                    + ", "
                    + "name(urn:oid:2.5.4.3)=[Peter Ng](len=1)"
                    + ", "
                    + "groups(urn:oid:1.3.6.1.4.1.5923.1.5.1.1)=[employees, engineering, managers](len=3)"
                    + "]}"
                    + "{["
                    + "mail(urn:oid:0.9.2342.19200300.100.1.3)=[1 value(s)]"
                    + "]}"
            )
        );
    }

}
