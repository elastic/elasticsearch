/* See LICENSE for licensing and NOTICE for copyright. */

package org.cryptacular.util;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.GeneralNamesBuilder;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.PolicyInformation;
import org.cryptacular.CryptUtil;
import org.cryptacular.EncodingException;
import org.cryptacular.StreamException;
import org.cryptacular.codec.Base64Encoder;
import org.cryptacular.x509.ExtensionReader;
import org.cryptacular.x509.GeneralNameType;
import org.cryptacular.x509.KeyUsageBits;
import org.cryptacular.x509.dn.NameReader;
import org.cryptacular.x509.dn.StandardAttributeType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.security.auth.x500.X500Principal;

/**
 * Utility class providing convenience methods for common operations on X.509 certificates.
 *
 * @author  Middleware Services
 */
public final class CertUtil {

    /** Private constructor of utility class. */
    private CertUtil() {}

    /**
     * Gets the common name attribute (CN) of the certificate subject distinguished name.
     *
     * @param  cert  Certificate to examine.
     *
     * @return  Subject CN or null if no CN attribute is defined in the subject DN.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static String subjectCN(final X509Certificate cert) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        return new NameReader(cert).readSubject().getValue(StandardAttributeType.CommonName);
    }

    /**
     * Gets all subject alternative names defined on the given certificate.
     *
     * @param  cert  X.509 certificate to examine.
     *
     * @return  List of subject alternative names or null if no subject alt names are defined.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static GeneralNames subjectAltNames(final X509Certificate cert) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        return new ExtensionReader(cert).readSubjectAlternativeName();
    }

    /**
     * Gets all subject alternative names of the given type(s) on the given cert.
     *
     * @param  cert  X.509 certificate to examine.
     * @param  types  One or more subject alternative name types to fetch.
     *
     * @return  List of subject alternative names of the matching type(s) or null if none found.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static GeneralNames subjectAltNames(final X509Certificate cert, final GeneralNameType... types) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        CryptUtil.assertNotNullArg(types, "Types cannot be null");
        final GeneralNamesBuilder builder = new GeneralNamesBuilder();
        final GeneralNames altNames = subjectAltNames(cert);
        if (altNames != null) {
            for (GeneralName name : altNames.getNames()) {
                for (GeneralNameType type : types) {
                    if (type.ordinal() == name.getTagNo()) {
                        builder.addName(name);
                    }
                }
            }
        }

        final GeneralNames names = builder.build();
        if (names.getNames().length == 0) {
            return null;
        }
        return names;
    }

    /**
     * Gets a list of all subject names defined for the given certificate. The list includes the first common name (CN)
     * specified in the subject distinguished name (if defined) and all subject alternative names.
     *
     * @param  cert  X.509 certificate to examine.
     *
     * @return  List of subject names.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static List<String> subjectNames(final X509Certificate cert) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        final List<String> names = new ArrayList<>();
        final String cn = subjectCN(cert);
        if (cn != null) {
            names.add(cn);
        }

        final GeneralNames altNames = subjectAltNames(cert);
        if (altNames == null) {
            return names;
        }
        for (GeneralName name : altNames.getNames()) {
            names.add(name.getName().toString());
        }
        return names;
    }

    /**
     * Gets a list of subject names defined for the given certificate. The list includes the first common name (CN)
     * specified in the subject distinguished name (if defined) and all subject alternative names of the given type.
     *
     * @param  cert  X.509 certificate to examine.
     * @param  types  One or more subject alternative name types to fetch.
     *
     * @return  List of subject names.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static List<String> subjectNames(final X509Certificate cert, final GeneralNameType... types) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        CryptUtil.assertNotNullArg(types, "Types cannot be null");
        final List<String> names = new ArrayList<>();
        final String cn = subjectCN(cert);
        if (cn != null) {
            names.add(cn);
        }

        final GeneralNames altNames = subjectAltNames(cert, types);
        if (altNames == null) {
            return names;
        }
        for (GeneralName name : altNames.getNames()) {
            names.add(name.getName().toString());
        }
        return names;
    }

    /**
     * Finds a certificate whose public key is paired with the given private key.
     *
     * @param  key  Private key used to find matching public key.
     * @param  candidates  Array of candidate certificates.
     *
     * @return  Certificate whose public key forms a keypair with the private key or null if no match is found.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static X509Certificate findEntityCertificate(final PrivateKey key, final X509Certificate... candidates)
        throws EncodingException {
        CryptUtil.assertNotNullArg(key, "Private key cannot be null");
        CryptUtil.assertNotNullArg(candidates, "Certificates cannot be null");
        return findEntityCertificate(key, Arrays.asList(candidates));
    }

    /**
     * Finds a certificate whose public key is paired with the given private key.
     *
     * @param  key  Private key used to find matching public key.
     * @param  candidates  Collection of candidate certificates.
     *
     * @return  Certificate whose public key forms a keypair with the private key or null if no match is found.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static X509Certificate findEntityCertificate(final PrivateKey key, final Collection<X509Certificate> candidates)
        throws EncodingException {
        CryptUtil.assertNotNullArg(key, "Private key cannot be null");
        CryptUtil.assertNotNullArg(candidates, "Certificates cannot be null");
        for (X509Certificate candidate : candidates) {
            if (KeyPairUtil.isKeyPair(candidate.getPublicKey(), key)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Reads an X.509 certificate from ASN.1 encoded format in the file at the given location.
     *
     * @param  path  Path to file containing an DER or PEM encoded X.509 certificate.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     * @throws  StreamException  on IO errors.
     */
    public static X509Certificate readCertificate(final String path) throws EncodingException, StreamException {
        return readCertificate(StreamUtil.makeStream(new File(CryptUtil.assertNotNullArg(path, "Path cannot be null"))));
    }

    /**
     * Reads an X.509 certificate from ASN.1 encoded format from the given file.
     *
     * @param  file  File containing an DER or PEM encoded X.509 certificate.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     * @throws  StreamException  on IO errors.
     */
    public static X509Certificate readCertificate(final File file) throws EncodingException, StreamException {
        return readCertificate(StreamUtil.makeStream(CryptUtil.assertNotNullArg(file, "File cannot be null")));
    }

    /**
     * Reads an X.509 certificate from ASN.1 encoded data in the given stream.
     *
     * @param  in  Input stream containing PEM or DER encoded X.509 certificate.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     * @throws  StreamException  on IO errors.
     */
    public static X509Certificate readCertificate(final InputStream in) throws EncodingException, StreamException {
        CryptUtil.assertNotNullArg(in, "Input stream cannot be null");
        try {
            final CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        } catch (CertificateException e) {
            if (e.getCause() instanceof IOException) {
                throw new StreamException((IOException) e.getCause());
            }
            throw new EncodingException("Cannot decode certificate", e);
        }
    }

    /**
     * Creates an X.509 certificate from its ASN.1 encoded form.
     *
     * @param  encoded  PEM or DER encoded ASN.1 data.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     */
    public static X509Certificate decodeCertificate(final byte[] encoded) throws EncodingException {
        return readCertificate(new ByteArrayInputStream(CryptUtil.assertNotNullArg(encoded, "Encoded certificate cannot be null")));
    }

    /**
     * Reads an X.509 certificate chain from ASN.1 encoded format in the file at the given location.
     *
     * @param  path  Path to file containing a sequence of PEM or DER encoded certificates or PKCS#7 certificate chain.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     * @throws  StreamException  on IO errors.
     */
    public static X509Certificate[] readCertificateChain(final String path) throws EncodingException, StreamException {
        return readCertificateChain(StreamUtil.makeStream(new File(CryptUtil.assertNotNullArg(path, "Path cannot be null"))));
    }

    /**
     * Reads an X.509 certificate chain from ASN.1 encoded format from the given file.
     *
     * @param  file  File containing a sequence of PEM or DER encoded certificates or PKCS#7 certificate chain.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     * @throws  StreamException  on IO errors.
     */
    public static X509Certificate[] readCertificateChain(final File file) throws EncodingException, StreamException {
        return readCertificateChain(StreamUtil.makeStream(CryptUtil.assertNotNullArg(file, "File cannot be null")));
    }

    /**
     * Reads an X.509 certificate chain from ASN.1 encoded data in the given stream.
     *
     * @param  in  Input stream containing a sequence of PEM or DER encoded certificates or PKCS#7 certificate chain.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     * @throws  StreamException  on IO errors.
     */
    public static X509Certificate[] readCertificateChain(final InputStream in) throws EncodingException, StreamException {
        CryptUtil.assertNotNullArg(in, "Input stream cannot be null");
        try {
            final CertificateFactory factory = CertificateFactory.getInstance("X.509");
            final Collection<? extends Certificate> certs = factory.generateCertificates(in);
            return certs.toArray(new X509Certificate[0]);
        } catch (CertificateException e) {
            if (e.getCause() instanceof IOException) {
                throw new StreamException((IOException) e.getCause());
            }
            throw new EncodingException("Cannot decode certificate", e);
        }
    }

    /**
     * Creates an X.509 certificate chain from its ASN.1 encoded form.
     *
     * @param  encoded  Sequence of PEM or DER encoded certificates or PKCS#7 certificate chain.
     *
     * @return  Certificate.
     *
     * @throws  EncodingException  on cert parsing errors.
     */
    public static X509Certificate[] decodeCertificateChain(final byte[] encoded) throws EncodingException {
        return readCertificateChain(
            new ByteArrayInputStream(CryptUtil.assertNotNullArg(encoded, "Encoded certificate chain cannot be null"))
        );
    }

    /**
     * Determines whether the certificate allows the given basic key usages.
     *
     * @param  cert  Certificate to check.
     * @param  bits  One or more basic key usage types to check.
     *
     * @return  True if certificate allows all given usage types, false otherwise.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static boolean allowsUsage(final X509Certificate cert, final KeyUsageBits... bits) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        CryptUtil.assertNotNullArg(bits, "Key usage bits cannot be null");
        final KeyUsage usage = new ExtensionReader(cert).readKeyUsage();
        for (KeyUsageBits bit : bits) {
            if (!bit.isSet(usage)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether the certificate allows the given extended key usages.
     *
     * @param  cert  Certificate to check.
     * @param  purposes  One or more extended key usage purposes to check.
     *
     * @return  True if certificate allows all given purposes, false otherwise.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static boolean allowsUsage(final X509Certificate cert, final KeyPurposeId... purposes) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        CryptUtil.assertNotNullArg(purposes, "Purposes cannot be null");
        final List<KeyPurposeId> allowedUses = new ExtensionReader(cert).readExtendedKeyUsage();
        for (KeyPurposeId purpose : purposes) {
            if (allowedUses == null || !allowedUses.contains(purpose)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether the certificate defines all the given certificate policies.
     *
     * @param  cert  Certificate to check.
     * @param  policyOidsToCheck  One or more certificate policy OIDs to check.
     *
     * @return  True if certificate defines all given policy OIDs, false otherwise.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static boolean hasPolicies(final X509Certificate cert, final String... policyOidsToCheck) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        CryptUtil.assertNotNullArg(policyOidsToCheck, "Policy OIDs to check cannot be null");
        final List<PolicyInformation> policies = new ExtensionReader(cert).readCertificatePolicies();
        boolean hasPolicy;
        for (String policyOid : policyOidsToCheck) {
            hasPolicy = false;
            if (policies != null) {
                for (PolicyInformation policy : policies) {
                    if (policy.getPolicyIdentifier().getId().equals(policyOid)) {
                        hasPolicy = true;
                        break;
                    }
                }
            }
            if (!hasPolicy) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets the subject key identifier of the given certificate in delimited hexadecimal format, e.g. <code>
     * 25:48:2f:28:ec:5d:19:bb:1d:25:ae:94:93:b1:7b:b5:35:96:24:66</code>.
     *
     * @param  cert  Certificate to process.
     *
     * @return  Subject key identifier in colon-delimited hex format.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static String subjectKeyId(final X509Certificate cert) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        return CodecUtil.hex(new ExtensionReader(cert).readSubjectKeyIdentifier().getKeyIdentifier(), true);
    }

    /**
     * Gets the authority key identifier of the given certificate in delimited hexadecimal format, e.g. <code>
     * 25:48:2f:28:ec:5d:19:bb:1d:25:ae:94:93:b1:7b:b5:35:96:24:66</code>.
     *
     * @param  cert  Certificate to process.
     *
     * @return  Authority key identifier in colon-delimited hex format.
     *
     * @throws  EncodingException  on cert field extraction.
     */
    public static String authorityKeyId(final X509Certificate cert) throws EncodingException {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        return CodecUtil.hex(new ExtensionReader(cert).readAuthorityKeyIdentifier().getKeyIdentifierOctets(), true);
    }

    /**
     * PEM encodes the given certificate with the provided encoding type.
     *
     * @param <T> type of encoding
     *
     * @param cert X.509 certificate.
     * @param encodeType Type of encoding. {@link EncodeType#X509} or {@link EncodeType#PKCS7}
     *
     * @return either DER encoded certificate or PEM-encoded certificate header and footer defined by {@link EncodeType}
     * and data wrapped at 64 characters per line.
     *
     * @throws RuntimeException if a certificate encoding error occurs
     */
    public static <T> T encodeCert(final X509Certificate cert, final EncodeType<T> encodeType) {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        CryptUtil.assertNotNullArg(encodeType, "Encode type cannot be null");
        try {
            return encodeType.encode(cert);
        } catch (CertificateEncodingException e) {
            throw new RuntimeException("Error getting encoded X.509 certificate data", e);
        }
    }

    /**
     * Retrieves the subject distinguished name (DN) of the provided X.509 certificate.
     * The subject DN represents the identity of the certificate holder and typically includes information
     * such as the common name (CN), organizational unit (OU), organization (O), locality (L), state (ST),
     * country (C), and other attributes.
     *
     * @param cert   The X.509 certificate from which to extract the subject DN.
     * @param format Controls whether the output contains spaces between attributes in the DN.
     *               Use {@link X500PrincipalFormat#READABLE} to generate a DN with spaces after the commas separating
     *               attribute-value pairs, {@link X500PrincipalFormat#RFC2253} for no spaces.
     * @return The subject DN string of the X.509 certificate.
     *
     * @throws NullPointerException  If the provided certificate is null.
     */
    public static String subjectDN(final X509Certificate cert, final X500PrincipalFormat format) {
        CryptUtil.assertNotNullArg(cert, "Certificate cannot be null");
        final X500Principal subjectX500Principal = cert.getSubjectX500Principal();
        return X500PrincipalFormat.READABLE.equals(format)
            ? subjectX500Principal.toString()
            : subjectX500Principal.getName(X500Principal.RFC2253);
    }

    /**
     * Generates a self-signed certificate.
     *
     * @param keyPair used for signing the certificate
     * @param dn Subject dn
     * @param duration Validity period of the certificate. The <em>notAfter</em> field is set to {@code now}
     * plus this value.
     * @param signatureAlgo the signature algorithm identifier to use
     *
     * @return a self-signed X509Certificate
     */
    public static X509Certificate generateX509Certificate(
        final KeyPair keyPair,
        final String dn,
        final Duration duration,
        final String signatureAlgo
    ) {
        throw new UnsupportedOperationException("Certificate generation not supported in this build");
    }

    /**
     * Generates a self-signed certificate.
     *
     * @param keyPair used for signing the certificate
     * @param dn Subject dn
     * @param notBefore the date and time when the certificate validity period starts
     * @param notAfter  the date and time when the certificate validity period ends
     * @param signatureAlgo the signature algorithm identifier to use
     *
     * @return a self-signed X509Certificate
     */
    public static X509Certificate generateX509Certificate(
        final KeyPair keyPair,
        final String dn,
        final Date notBefore,
        final Date notAfter,
        final String signatureAlgo
    ) {
        throw new UnsupportedOperationException("Certificate generation not supported in this build");
    }

    /**
     * Describes the behavior of string formatting of X.500 distinguished names.
     */
    public enum X500PrincipalFormat {
        /** The format described in RFC2253 (without spaces). */
        RFC2253,

        /** Similar to RFC2253, but with spaces. */
        READABLE
    }

    /**
     * Marker interface for encoding types.
     *
     * @param <T> type of encoding
     */
    public interface EncodeType<T> {

        /** DER encode type.*/
        EncodeType<byte[]> DER = new DEREncodeType();

        /** X509 encode type. */
        EncodeType<String> X509 = new X509EncodeType();

        /** PKCS7 encode type. */
        EncodeType<String> PKCS7 = new PKCS7EncodeType();

        /**
         * Returns the type of encoding.
         *
         * @return type
         */
        String getType();

        /**
         * Encodes the supplied certificate.
         *
         * @param cert to encode
         *
         * @return encoded certificate
         *
         * @throws CertificateEncodingException if an error occurs encoding the certificate
         */
        T encode(X509Certificate cert) throws CertificateEncodingException;
    }

    /**
     * Base implementation for PEM encoded types.
     */
    private abstract static class AbstractPemEncodeType implements EncodeType<String> {

        /**
         * Returns a PEM encoding of the supplied DER bytes.
         *
         * @param der to encode
         *
         * @return PEM encoded certificate
         */
        protected String encodePem(final byte[] der) {
            final Base64Encoder encoder = new Base64Encoder(64);
            final ByteBuffer input = ByteBuffer.wrap(CryptUtil.assertNotNullArg(der, "DER cannot be null"));
            // Space for Base64-encoded data + header, footer, line breaks, and potential padding
            final CharBuffer output = CharBuffer.allocate(encoder.outputSize(der.length) + 100);
            output.append("-----BEGIN ").append(getType()).append("-----");
            output.append(System.lineSeparator());
            encoder.encode(input, output);
            encoder.finalize(output);
            output.flip();
            return output.toString().trim().concat(System.lineSeparator()).concat("-----END ").concat(getType()).concat("-----");
        }
    }

    /** DER encode type. */
    private static final class DEREncodeType implements EncodeType<byte[]> {

        @Override
        public String getType() {
            return "DER";
        }

        @Override
        public byte[] encode(final X509Certificate cert) throws CertificateEncodingException {
            return CryptUtil.assertNotNullArg(cert, "Certificate cannot be null").getEncoded();
        }
    }

    /** X509 encode type. */
    private static final class X509EncodeType extends AbstractPemEncodeType {

        @Override
        public String getType() {
            return "CERTIFICATE";
        }

        @Override
        public String encode(final X509Certificate cert) throws CertificateEncodingException {
            return encodePem(CryptUtil.assertNotNullArg(cert, "Certificate cannot be null").getEncoded());
        }
    }

    /** PKCS7 encode type. */
    private static final class PKCS7EncodeType extends AbstractPemEncodeType {

        @Override
        public String getType() {
            return "PKCS7";
        }

        @Override
        public String encode(final X509Certificate cert) throws CertificateEncodingException {
            return encodePem(CryptUtil.assertNotNullArg(cert, "Certificate cannot be null").getEncoded());
        }
    }
}
