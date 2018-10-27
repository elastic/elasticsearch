/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.RequestAbstractType;
import org.opensaml.saml.saml2.core.StatusResponseType;
import org.opensaml.xmlsec.signature.support.SignatureConstants;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class SamlRedirect {

    private final SAMLObject samlObject;
    private final String destination;
    private final String parameterName;
        private final SigningConfiguration signing;

    public SamlRedirect(RequestAbstractType request, SigningConfiguration signing) {
        this.samlObject = request;
        this.destination = request.getDestination();
        this.parameterName = "SAMLRequest";
        this.signing = signing;
    }

    public SamlRedirect(StatusResponseType response, SigningConfiguration signing) {
        this.samlObject = response;
        this.destination = response.getDestination();
        this.parameterName = "SAMLResponse";
        this.signing = signing;
    }

    public String getRedirectUrl() throws ElasticsearchException {
        return getRedirectUrl(null);
    }

    public String getRedirectUrl(String relayState) throws ElasticsearchException {
        try {
            final String request = deflateAndBase64Encode(this.samlObject);
            String queryParam = parameterName + "=" + urlEncode(request);
            if (relayState != null) {
                queryParam += "&RelayState=" + urlEncode(relayState);
            }
            if (signing.shouldSign(this.samlObject)) {
                final String algo = SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256;
                queryParam += "&SigAlg=" + urlEncode(algo);
                final byte[] sig = signing.sign(queryParam, algo);
                queryParam += "&Signature=" + urlEncode(base64Encode(sig));
            }
            return withParameters(queryParam);
        } catch (Exception e) {
            throw new ElasticsearchException("Cannot construct SAML redirect", e);
        }
    }

    private String withParameters(String queryParam) {
        if (destination.indexOf('?') == -1) {
            return destination + "?" + queryParam;
        } else if (destination.endsWith("?")) {
            return destination + queryParam;
        } else {
            return destination + "&" + queryParam;
        }
    }

    private String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private String urlEncode(String param) throws UnsupportedEncodingException {
        return URLEncoder.encode(param, StandardCharsets.US_ASCII.name());
    }

    protected String deflateAndBase64Encode(SAMLObject message)
            throws Exception {
        Deflater deflater = new Deflater(Deflater.DEFLATED, true);
        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             DeflaterOutputStream deflaterStream = new DeflaterOutputStream(bytesOut, deflater)) {
            String messageStr = SamlUtils.toString(XMLObjectSupport.marshall(message));
            deflaterStream.write(messageStr.getBytes(StandardCharsets.UTF_8));
            deflaterStream.finish();
            return base64Encode(bytesOut.toByteArray());
        }
    }
}
