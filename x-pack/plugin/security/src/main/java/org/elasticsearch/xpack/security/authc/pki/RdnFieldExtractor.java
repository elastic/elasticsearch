/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.common.ssl.DerParser;

import java.io.IOException;

/**
 * Utility class to extract RDN field values from X500 principal DER encoding.
 */
public class RdnFieldExtractor {

    public static String extract(byte[] encoded, String oid) {
        try {
            return doExtract(encoded, oid);
        } catch (IOException e) {
            return null;
        }
    }

    private static String doExtract(byte[] encoded, String oid) throws IOException {
        DerParser parser = new DerParser(encoded);

        DerParser.Asn1Object dnSequence = parser.readAsn1Object();
        DerParser sequenceParser = dnSequence.getParser();

        while (true) {
            DerParser.Asn1Object rdnSet = sequenceParser.readAsn1Object();
            if (rdnSet.isConstructed() == false) {
                return null;
            }

            DerParser setParser = rdnSet.getParser();
            DerParser.Asn1Object attrSeq = setParser.readAsn1Object();

            if (attrSeq.isConstructed()) {
                DerParser attrParser = attrSeq.getParser();
                String attrOid = attrParser.readAsn1Object().getOid();

                if (oid.equals(attrOid)) {
                    try {
                        setParser.readAsn1Object();
                        return null; // abort if additional attributes on the RDN
                    } catch (IOException e) {
                        return attrParser.readAsn1Object().getString(); // success if just one attribute in sequence
                    }
                }
            }
        }
    }

}
