/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Subject;

/**
 * Lightweight (non-XML) representation of a SAML {@code NameID} element
 */
public class SamlNameId {
    final String format;
    final String value;
    final String idpNameQualifier;
    final String spNameQualifier;
    final String spProvidedId;

    public SamlNameId(String format, String value, String idpNameQualifier, String spNameQualifier, String spProvidedId) {
        this.format = format;
        this.value = value;
        this.idpNameQualifier = idpNameQualifier;
        this.spNameQualifier = spNameQualifier;
        this.spProvidedId = spProvidedId;
    }

    @Override
    public String toString() {
        return "NameId(" + format + ")=" + value;
    }

    public NameID asXml() {
        final NameID nameId = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameId.setFormat(format);
        nameId.setValue(value);
        nameId.setNameQualifier(idpNameQualifier);
        nameId.setSPNameQualifier(spNameQualifier);
        nameId.setSPProvidedID(spProvidedId);
        return nameId;
    }

    static SamlNameId fromXml(NameID name) {
        if (name == null) {
            return null;
        }
        return new SamlNameId(name.getFormat(), name.getValue(), name.getNameQualifier(),
                name.getSPNameQualifier(), name.getSPProvidedID());
    }

    static SamlNameId forSubject(Subject subject) {
        if (subject == null) {
            return null;
        }
        final NameID name = subject.getNameID();
        return fromXml(name);
    }

}
