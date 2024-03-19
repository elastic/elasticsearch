/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.KeyStoreAwareCommand;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.security.authc.saml.SamlSpMetadataBuilder.ContactInfo;
import org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureConstants;
import org.opensaml.xmlsec.signature.support.Signer;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * CLI tool to generate SAML Metadata for a Service Provider (realm)
 */
class SamlMetadataCommand extends KeyStoreAwareCommand {

    static final String METADATA_SCHEMA = "saml-schema-metadata-2.0.xsd";

    private final OptionSpec<String> outputPathSpec;
    private final OptionSpec<Void> batchSpec;
    private final OptionSpec<String> realmSpec;
    private final OptionSpec<String> localeSpec;
    private final OptionSpec<String> serviceNameSpec;
    private final OptionSpec<String> attributeSpec;
    private final OptionSpec<String> orgNameSpec;
    private final OptionSpec<String> orgDisplayNameSpec;
    private final OptionSpec<String> orgUrlSpec;
    private final OptionSpec<Void> contactsSpec;
    private final OptionSpec<String> signingPkcs12PathSpec;
    private final OptionSpec<String> signingCertPathSpec;
    private final OptionSpec<String> signingKeyPathSpec;
    private final OptionSpec<String> keyPasswordSpec;
    private final CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction;
    private KeyStoreWrapper keyStoreWrapper;

    SamlMetadataCommand() {
        this((environment) -> {
            KeyStoreWrapper ksWrapper = KeyStoreWrapper.load(environment.configFile());
            return ksWrapper;
        });
    }

    SamlMetadataCommand(CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction) {
        super("Generate Service Provider Metadata for a SAML realm");
        outputPathSpec = parser.accepts("out", "path of the xml file that should be generated").withRequiredArg();
        batchSpec = parser.accepts("batch", "Do not prompt");
        realmSpec = parser.accepts("realm", "name of the elasticsearch realm for which metadata should be generated").withRequiredArg();
        localeSpec = parser.accepts("locale", "the locale to be used for elements that require a language").withRequiredArg();
        serviceNameSpec = parser.accepts("service-name", "the name to apply to the attribute consuming service").withRequiredArg();
        attributeSpec = parser.accepts("attribute", "additional SAML attributes to request").withRequiredArg();
        orgNameSpec = parser.accepts("organisation-name", "the name of the organisation operating this service").withRequiredArg();
        orgDisplayNameSpec = parser.accepts("organisation-display-name", "the display-name of the organisation operating this service")
            .availableIf(orgNameSpec)
            .withRequiredArg();
        orgUrlSpec = parser.accepts("organisation-url", "the URL of the organisation operating this service")
            .requiredIf(orgNameSpec)
            .withRequiredArg();
        contactsSpec = parser.accepts("contacts", "Include contact information in metadata").availableUnless(batchSpec);
        signingPkcs12PathSpec = parser.accepts(
            "signing-bundle",
            "path to an existing key pair (in PKCS#12 format) to be used for " + "signing "
        ).withRequiredArg();
        signingCertPathSpec = parser.accepts("signing-cert", "path to an existing signing certificate")
            .availableUnless(signingPkcs12PathSpec)
            .withRequiredArg();
        signingKeyPathSpec = parser.accepts("signing-key", "path to an existing signing private key")
            .availableIf(signingCertPathSpec)
            .requiredIf(signingCertPathSpec)
            .withRequiredArg();
        keyPasswordSpec = parser.accepts("signing-key-password", "password for an existing signing private key or keypair")
            .withOptionalArg();
        this.keyStoreFunction = keyStoreFunction;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (keyStoreWrapper != null) {
            keyStoreWrapper.close();
        }
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        // OpenSAML prints a lot of _stuff_ at info level, that really isn't needed in a command line tool.
        Loggers.setLevel(LogManager.getLogger("org.opensaml"), Level.WARN);

        final Logger logger = LogManager.getLogger(getClass());
        SamlUtils.initialize(logger);

        final EntityDescriptor descriptor = buildEntityDescriptor(terminal, options, env);
        Element element = possiblySignDescriptor(terminal, options, descriptor, env);

        final Path xml = writeOutput(terminal, options, element);
        validateXml(terminal, xml);
    }

    // package-protected for testing
    EntityDescriptor buildEntityDescriptor(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final boolean batch = options.has(batchSpec);

        final RealmConfig realm = findRealm(terminal, options, env);
        final Settings realmSettings = realm.settings().getByPrefix(RealmSettings.realmSettingPrefix(realm.identifier()));
        terminal.println(
            Terminal.Verbosity.VERBOSE,
            "Using realm configuration\n=====\n" + realmSettings.toDelimitedString('\n') + "====="
        );
        final Locale locale = findLocale(options);
        terminal.println(Terminal.Verbosity.VERBOSE, "Using locale: " + locale.toLanguageTag());

        final SpConfiguration spConfig = SamlRealm.getSpConfiguration(realm);
        final SamlSpMetadataBuilder builder = new SamlSpMetadataBuilder(locale, spConfig.getEntityId()).assertionConsumerServiceUrl(
            spConfig.getAscUrl()
        )
            .singleLogoutServiceUrl(spConfig.getLogoutUrl())
            .encryptionCredentials(spConfig.getEncryptionCredentials())
            .signingCredential(spConfig.getSigningConfiguration().getCredential())
            .authnRequestsSigned(spConfig.getSigningConfiguration().shouldSign(AuthnRequest.DEFAULT_ELEMENT_LOCAL_NAME))
            .nameIdFormat(realm.getSetting(SamlRealmSettings.NAMEID_FORMAT))
            .serviceName(option(serviceNameSpec, options, env.settings().get("cluster.name")));

        Map<String, String> attributes = getAttributeNames(options, realm);
        for (String attr : attributes.keySet()) {
            final String name;
            String friendlyName;
            final String settingName = attributes.get(attr);
            final String attributeSource = settingName == null ? "command line" : '"' + settingName + '"';
            if (attr.contains(":")) {
                name = attr;
                if (batch) {
                    friendlyName = settingName;
                } else {
                    friendlyName = terminal.readText(
                        "What is the friendly name for "
                            + attributeSource
                            + " attribute \""
                            + attr
                            + "\" [default: "
                            + (settingName == null ? "none" : settingName)
                            + "] "
                    );
                    if (Strings.isNullOrEmpty(friendlyName)) {
                        friendlyName = settingName;
                    }
                }
            } else {
                if (batch) {
                    throw new UserException(
                        ExitCodes.CONFIG,
                        "Option " + batchSpec.toString() + " is specified, but attribute " + attr + " appears to be a FriendlyName value"
                    );
                }
                friendlyName = attr;
                name = requireText(
                    terminal,
                    "What is the standard (urn) name for " + attributeSource + " attribute \"" + attr + "\" (required): "
                );
            }
            terminal.println(Terminal.Verbosity.VERBOSE, "Requesting attribute '" + name + "' (FriendlyName: '" + friendlyName + "')");
            builder.withAttribute(friendlyName, name);
        }

        if (options.has(orgNameSpec) && options.has(orgUrlSpec)) {
            String name = orgNameSpec.value(options);
            builder.organization(name, option(orgDisplayNameSpec, options, name), orgUrlSpec.value(options));
        }

        if (options.has(contactsSpec)) {
            terminal.println("\nPlease enter the personal details for each contact to be included in the metadata");
            do {
                final String givenName = requireText(terminal, "What is the given name for the contact: ");
                final String surName = requireText(terminal, "What is the surname for the contact: ");
                final String displayName = givenName + ' ' + surName;
                final String email = requireText(terminal, "What is the email address for " + displayName + ": ");
                String type;
                while (true) {
                    type = requireText(terminal, "What is the contact type for " + displayName + ": ");
                    if (ContactInfo.TYPES.containsKey(type)) {
                        break;
                    } else {
                        terminal.errorPrintln(
                            "Type '"
                                + type
                                + "' is not valid. Valid values are "
                                + Strings.collectionToCommaDelimitedString(ContactInfo.TYPES.keySet())
                        );
                    }
                }
                builder.withContact(type, givenName, surName, email);
            } while (terminal.promptYesNo("Enter details for another contact", true));
        }

        return builder.build();
    }

    // package-protected for testing
    Element possiblySignDescriptor(Terminal terminal, OptionSet options, EntityDescriptor descriptor, Environment env)
        throws UserException {
        try {
            final EntityDescriptorMarshaller marshaller = new EntityDescriptorMarshaller();
            if (options.has(signingPkcs12PathSpec) || (options.has(signingCertPathSpec) && options.has(signingKeyPathSpec))) {
                Signature signature = (Signature) XMLObjectProviderRegistrySupport.getBuilderFactory()
                    .getBuilder(Signature.DEFAULT_ELEMENT_NAME)
                    .buildObject(Signature.DEFAULT_ELEMENT_NAME);
                signature.setSigningCredential(buildSigningCredential(terminal, options, env));
                signature.setSignatureAlgorithm(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256);
                signature.setCanonicalizationAlgorithm(SignatureConstants.ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
                descriptor.setSignature(signature);
                Element element = marshaller.marshall(descriptor);
                Signer.signObject(signature);
                return element;
            } else {
                return marshaller.marshall(descriptor);
            }
        } catch (Exception e) {
            String errorMessage;
            if (e instanceof MarshallingException) {
                errorMessage = "Error serializing Metadata to file";
            } else if (e instanceof org.opensaml.xmlsec.signature.support.SignatureException) {
                errorMessage = "Error attempting to sign Metadata";
            } else {
                errorMessage = "Error building signing credentials from provided keyPair";
            }
            terminal.errorPrintln(Terminal.Verbosity.SILENT, errorMessage);
            terminal.errorPrintln("The following errors were found:");
            printExceptions(terminal, e);
            throw new UserException(ExitCodes.CANT_CREATE, "Unable to create metadata document");
        }
    }

    private Path writeOutput(Terminal terminal, OptionSet options, Element element) throws Exception {
        final Path outputFile = resolvePath(option(outputPathSpec, options, "saml-elasticsearch-metadata.xml"));
        final Writer writer = Files.newBufferedWriter(outputFile);
        SamlUtils.print(element, writer, true);
        terminal.println("\nWrote SAML metadata to " + outputFile);
        return outputFile;
    }

    private Credential buildSigningCredential(Terminal terminal, OptionSet options, Environment env) throws Exception {
        X509Certificate signingCertificate;
        PrivateKey signingKey;
        char[] password = getChars(keyPasswordSpec.value(options));
        if (options.has(signingPkcs12PathSpec)) {
            Path p12Path = resolvePath(signingPkcs12PathSpec.value(options));
            Map<Certificate, Key> keys = withPassword(
                "certificate bundle (" + p12Path + ")",
                password,
                terminal,
                keyPassword -> CertParsingUtils.readPkcs12KeyPairs(p12Path, keyPassword, a -> keyPassword)
            );

            if (keys.size() != 1) {
                throw new IllegalArgumentException(
                    "expected a single key in file [" + p12Path.toAbsolutePath() + "] but found [" + keys.size() + "]"
                );
            }
            final Map.Entry<Certificate, Key> pair = keys.entrySet().iterator().next();
            signingCertificate = (X509Certificate) pair.getKey();
            signingKey = (PrivateKey) pair.getValue();
        } else {
            Path cert = resolvePath(signingCertPathSpec.value(options));
            Path key = resolvePath(signingKeyPathSpec.value(options));
            signingCertificate = CertParsingUtils.readX509Certificate(cert);
            signingKey = readSigningKey(key, password, terminal);
        }
        return new BasicX509Credential(signingCertificate, signingKey);
    }

    private static <T, E extends Exception> T withPassword(
        String description,
        char[] password,
        Terminal terminal,
        CheckedFunction<char[], T, E> body
    ) throws E {
        if (password == null) {
            char[] promptedValue = terminal.readSecret("Enter password for " + description + " : ");
            try {
                return body.apply(promptedValue);
            } finally {
                Arrays.fill(promptedValue, (char) 0);
            }
        } else {
            return body.apply(password);
        }
    }

    private static char[] getChars(String password) {
        return password == null ? null : password.toCharArray();
    }

    private static PrivateKey readSigningKey(Path path, char[] password, Terminal terminal) throws Exception {
        AtomicReference<char[]> passwordReference = new AtomicReference<>(password);
        try {
            return PemUtils.readPrivateKey(path, () -> {
                if (password != null) {
                    return password;
                }
                char[] promptedValue = terminal.readSecret("Enter password for the signing key (" + path.getFileName() + ") : ");
                passwordReference.set(promptedValue);
                return promptedValue;
            });
        } finally {
            if (passwordReference.get() != null) {
                Arrays.fill(passwordReference.get(), (char) 0);
            }
        }
    }

    private static void validateXml(Terminal terminal, Path xml) throws Exception {
        try (InputStream xmlInput = Files.newInputStream(xml)) {
            SamlUtils.validate(xmlInput, METADATA_SCHEMA);
            terminal.println(Terminal.Verbosity.VERBOSE, "The generated metadata file conforms to the SAML metadata schema");
        } catch (SAXException e) {
            terminal.errorPrintln(
                Terminal.Verbosity.SILENT,
                "Error - The generated metadata file does not conform to the " + "SAML metadata schema"
            );
            terminal.errorPrintln("While validating " + xml.toString() + " the follow errors were found:");
            printExceptions(terminal, e);
            throw new UserException(ExitCodes.CODE_ERROR, "Generated metadata is not valid");
        }
    }

    private static void printExceptions(Terminal terminal, Throwable throwable) {
        terminal.errorPrintln(" - " + throwable.getMessage());
        for (Throwable sup : throwable.getSuppressed()) {
            printExceptions(terminal, sup);
        }
        if (throwable.getCause() != null && throwable.getCause() != throwable) {
            printExceptions(terminal, throwable.getCause());
        }
    }

    @SuppressForbidden(reason = "CLI tool working from current directory")
    private static Path resolvePath(String name) {
        return PathUtils.get(name).normalize();
    }

    private static String requireText(Terminal terminal, String prompt) {
        String value = null;
        while (Strings.isNullOrEmpty(value)) {
            value = terminal.readText(prompt);
        }
        return value;
    }

    private static <T> T option(OptionSpec<T> spec, OptionSet options, T defaultValue) {
        if (options.has(spec)) {
            return spec.value(options);
        } else {
            return defaultValue;
        }
    }

    /**
     * Map of saml-attribute name to configuration-setting name
     */
    private Map<String, String> getAttributeNames(OptionSet options, RealmConfig realm) {
        Map<String, String> attributes = new LinkedHashMap<>();
        for (String a : attributeSpec.values(options)) {
            attributes.put(a, null);
        }
        final String prefix = RealmSettings.realmSettingPrefix(realm.identifier()) + SamlRealmSettings.AttributeSetting.ATTRIBUTES_PREFIX;
        final Settings attributeSettings = realm.settings().getByPrefix(prefix);
        for (String key : sorted(attributeSettings.keySet())) {
            final String attr = attributeSettings.get(key);
            attributes.put(attr, key);
        }
        return attributes;
    }

    // We sort this Set so that it is deterministic for testing
    private static SortedSet<String> sorted(Set<String> strings) {
        return new TreeSet<>(strings);
    }

    /**
     * @TODO REALM-SETTINGS[TIM] This can be redone a lot now the realm settings are keyed by type
     */
    private RealmConfig findRealm(Terminal terminal, OptionSet options, Environment env) throws Exception {

        keyStoreWrapper = keyStoreFunction.apply(env);
        final Settings settings;
        if (keyStoreWrapper != null) {
            decryptKeyStore(keyStoreWrapper, terminal);

            final Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put(env.settings(), true);
            if (settingsBuilder.getSecureSettings() == null) {
                settingsBuilder.setSecureSettings(keyStoreWrapper);
            }
            settings = settingsBuilder.build();
        } else {
            settings = env.settings();
        }

        final Map<RealmConfig.RealmIdentifier, Settings> realms = RealmSettings.getRealmSettings(settings);
        if (options.has(realmSpec)) {
            final String name = realmSpec.value(options);
            final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(SamlRealmSettings.TYPE, name);
            final Settings realmSettings = realms.get(identifier);
            if (realmSettings == null) {
                throw new UserException(ExitCodes.CONFIG, "No such realm '" + name + "' defined in " + env.configFile());
            }
            if (isSamlRealm(identifier)) {
                return buildRealm(identifier, env, settings);
            } else {
                throw new UserException(ExitCodes.CONFIG, "Realm '" + name + "' is not a SAML realm (is '" + identifier.getType() + "')");
            }
        } else {
            final List<Map.Entry<RealmConfig.RealmIdentifier, Settings>> saml = realms.entrySet()
                .stream()
                .filter(entry -> isSamlRealm(entry.getKey()))
                .toList();
            if (saml.isEmpty()) {
                throw new UserException(ExitCodes.CONFIG, "There is no SAML realm configured in " + env.configFile());
            }
            if (saml.size() > 1) {
                terminal.errorPrintln("Using configuration in " + env.configFile());
                terminal.errorPrintln(
                    "Found multiple SAML realms: "
                        + saml.stream().map(Map.Entry::getKey).map(Object::toString).collect(Collectors.joining(", "))
                );
                terminal.errorPrintln("Use the -" + optionName(realmSpec) + " option to specify an explicit realm");
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Found multiple SAML realms, please specify one with '-" + optionName(realmSpec) + "'"
                );
            }
            final Map.Entry<RealmConfig.RealmIdentifier, Settings> entry = saml.get(0);
            terminal.println("Building metadata for SAML realm " + entry.getKey());
            return buildRealm(entry.getKey(), env, settings);
        }
    }

    private static String optionName(OptionSpec<?> spec) {
        return spec.options().get(0);
    }

    private static RealmConfig buildRealm(RealmConfig.RealmIdentifier identifier, Environment env, Settings globalSettings) {
        return new RealmConfig(identifier, globalSettings, env, new ThreadContext(globalSettings));
    }

    private static boolean isSamlRealm(RealmConfig.RealmIdentifier realmIdentifier) {
        return SamlRealmSettings.TYPE.equals(realmIdentifier.getType());
    }

    private Locale findLocale(OptionSet options) {
        if (options.has(localeSpec)) {
            return LocaleUtils.parse(localeSpec.value(options));
        } else {
            return Locale.getDefault();
        }
    }

    // For testing
    OptionParser getParser() {
        return parser;
    }
}
