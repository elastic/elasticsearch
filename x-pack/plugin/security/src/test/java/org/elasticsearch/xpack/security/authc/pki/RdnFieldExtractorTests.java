/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RdnFieldExtractorTests extends ESTestCase {

    private static final String OID_CN = "2.5.4.3";      // Common Name
    private static final String OID_OU = "2.5.4.11";     // Organizational Unit
    private static final String OID_O = "2.5.4.10";      // Organization
    private static final String OID_C = "2.5.4.6";       // Country
    private static final String OID_ST = "2.5.4.8";      // State or Province
    private static final String OID_L = "2.5.4.7";       // Locality
    private static final String OID_EMAIL = "1.2.840.113549.1.9.1"; // Email Address

    // Custom/domain-specific OID (fictional private enterprise OID)
    // Format: 1.3.6.1.4.1.<enterprise-number>.<custom-attributes>
    private static final String OID_EMPLOYEE_ID = "1.3.6.1.4.1.50000.1.1"; // Fictional: Employee ID

    private record ExtractionTestCase(String dn, String oid, String expectedValue) {}

    private static String extractFromDN(String dn, String oid) {
        X500Principal principal = new X500Principal(dn);
        return RdnFieldExtractor.extract(principal.getEncoded(), oid);
    }

    private void assertExtractions(String dn, Map<String, String> expectedExtractions) {
        byte[] encoded = new X500Principal(dn).getEncoded();
        for (Map.Entry<String, String> entry : expectedExtractions.entrySet()) {
            String oid = entry.getKey();
            String expected = entry.getValue();
            String actual = RdnFieldExtractor.extract(encoded, oid);
            assertThat("OID " + oid + " extraction failed for DN: " + dn, actual, is(equalTo(expected)));
        }
    }

    public void testExtractBasicAttributes() {
        List<ExtractionTestCase> testCases = List.of(
            new ExtractionTestCase("CN=John Doe, OU=Engineering, O=Elastic", OID_CN, "John Doe"),
            new ExtractionTestCase("CN=John Doe, OU=Engineering, O=Elastic", OID_OU, "Engineering"),
            new ExtractionTestCase("CN=John Doe, OU=Engineering, O=Elastic", OID_O, "Elastic"),
            new ExtractionTestCase("CN=John Doe, C=US", OID_C, "US"),
            new ExtractionTestCase("CN=John Doe, ST=California, C=US", OID_ST, "California"),
            new ExtractionTestCase("CN=John Doe, L=Mountain View, ST=California, C=US", OID_L, "Mountain View"),
            new ExtractionTestCase("EMAILADDRESS=john@elastic.co, CN=John Doe", OID_EMAIL, "john@elastic.co")
        );

        for (ExtractionTestCase testCase : testCases) {
            String result = extractFromDN(testCase.dn, testCase.oid);
            assertThat("Failed to extract from DN: " + testCase.dn, result, is(equalTo(testCase.expectedValue)));
        }
    }

    public void testExtractFirstOUWhenMultipleExist() {
        // When multiple RDNs with the same OID exist, should return the last one encountered in DER encoding
        // Note: X.500 encoding reverses the order - the last attribute in the DN string is first in DER encoding
        String ou = extractFromDN("CN=John Doe, OU=Security Team, OU=Engineering, O=Elastic", OID_OU);
        assertThat(ou, is(equalTo("Security Team")));
    }

    public void testExtractOidNotFound() {
        assertThat(extractFromDN("CN=John Doe, OU=Engineering", OID_C), is(nullValue()));
    }

    public void testExtractWithEmptyEncoding() {
        assertThat(RdnFieldExtractor.extract(new byte[0], OID_CN), is(nullValue()));
    }

    public void testExtractWithMalformedDerData() {
        byte[] malformedBytes = randomByteArrayOfLength(50);

        String result = RdnFieldExtractor.extract(malformedBytes, OID_CN);
        assertThat(result, is(nullValue()));
    }

    public void testExtractWithSpecialCharacters() {
        assertExtractions("CN=Test\\, User, OU=R\\+D, O=Elastic\\\\Co", Map.of(OID_CN, "Test, User", OID_OU, "R+D", OID_O, "Elastic\\Co"));
    }

    public void testExtractWithUtf8Characters() {
        assertExtractions(
            "CN=José García, OU=Ingeniería, O=Elástico",
            Map.of(OID_CN, "José García", OID_OU, "Ingeniería", OID_O, "Elástico")
        );
    }

    public void testExtractCustomDomainSpecificOid() {
        // Test with a custom OID that might be used in a private PKI infrastructure
        // This demonstrates the extractor works with any valid OID, not just RFC-standardized ones
        // Using OID format: 1.3.6.1.4.1.<enterprise-number>.<custom-attributes>
        String dnWithCustomOid = OID_EMPLOYEE_ID + "=EMP-2024-42, CN=Jane Developer, OU=Engineering, O=Acme Corp";
        String employeeId = extractFromDN(dnWithCustomOid, OID_EMPLOYEE_ID);
        assertThat("Custom domain-specific OID extraction failed", employeeId, is(equalTo("EMP-2024-42")));
    }

    public void testExtractFromMultiValuedRdn() {
        // Multi-valued RDNs use "+" to combine multiple attributes in a single RDN component (SET)
        // Example: "CN=John Doe+OU=Engineering" - both CN and OU are in the same RDN SET
        String multiValuedRdn = "CN=John Smith+OU=Development, O=Acme Corp";
        assertThat(extractFromDN(multiValuedRdn, OID_CN), is("John Smith"));
        assertThat(extractFromDN(multiValuedRdn, OID_OU), is("Development"));
        assertThat(extractFromDN(multiValuedRdn, OID_O), is("Acme Corp"));
    }
}
