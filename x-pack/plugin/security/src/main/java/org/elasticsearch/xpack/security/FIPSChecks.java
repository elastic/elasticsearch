package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.Locale;

import org.elasticsearch.bootstrap.FIPSContext;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.XPackSettings;

public class FIPSChecks implements FIPSInterface {
    
    static final EnumSet<License.OperationMode> ALLOWED_LICENSE_OPERATION_MODES =
            EnumSet.of(License.OperationMode.PLATINUM, License.OperationMode.TRIAL);

	@Override
	public FIPSCheckResult check(FIPSContext context, Environment env) {
		FIPSCheckResult check1 = keystoreCheck(context);
		FIPSCheckResult check2 = licenseCheck(context);
		FIPSCheckResult check3 = passwordHashingAlgorithmCheck(context);
		FIPSCheckResult check4 = secureSettingsCheck(context, env);
		
		if(check1.isSuccess() && check2.isSuccess() && check3.isSuccess() && check4.isSuccess()) {
			return FIPSCheckResult.success();
		}
		
		else {
			if(check1.isFailure()) {
				return check1;
			}
			
			if(check2.isFailure()) {
				return check2;
			}
			
			if(check3.isFailure()) {
				return check3;
			}
			
			if(check4.isFailure()) {
				return check4;
			}
		}
		
		return null;
	}
	
    public FIPSCheckResult keystoreCheck(FIPSContext context) {

        if (XPackSettings.FIPS_MODE_ENABLED.get(context.settings)) {
            final Settings settings = context.settings;
            Settings keystoreTypeSettings = settings.filter(k -> k.endsWith("keystore.type"))
                .filter(k -> settings.get(k).equalsIgnoreCase("jks"));
            if (keystoreTypeSettings.isEmpty() == false) {
                return FIPSCheckResult.failure("JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please " +
                    "revisit [" + keystoreTypeSettings.toDelimitedString(',') + "] settings");
            }
            // Default Keystore type is JKS if not explicitly set
            Settings keystorePathSettings = settings.filter(k -> k.endsWith("keystore.path"))
                .filter(k -> settings.hasValue(k.replace(".path", ".type")) == false);
            if (keystorePathSettings.isEmpty() == false) {
                return FIPSCheckResult.failure("JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please " +
                    "revisit [" + keystorePathSettings.toDelimitedString(',') + "] settings");
            }

        }
        
        return FIPSCheckResult.success();
    }
    
    public FIPSCheckResult licenseCheck(FIPSContext context) {
        if (XPackSettings.FIPS_MODE_ENABLED.get(context.settings)) {
            /*License license = LicenseService.getLicense(context.metaData);
            if (license != null && ALLOWED_LICENSE_OPERATION_MODES.contains(license.operationMode()) == false) {
                return FIPSCheckResult.failure("FIPS mode is only allowed with a Platinum or Trial license");
            } */
        }
        
        return FIPSCheckResult.success();
    }
    
    public FIPSCheckResult passwordHashingAlgorithmCheck(FIPSContext context) {
        if (XPackSettings.FIPS_MODE_ENABLED.get(context.settings)) {
            final String selectedAlgorithm = XPackSettings.PASSWORD_HASHING_ALGORITHM.get(context.settings);
            if (selectedAlgorithm.toLowerCase(Locale.ROOT).startsWith("pbkdf2") == false) {
                return FIPSCheckResult.failure("Only PBKDF2 is allowed for password hashing in a FIPS-140 JVM. Please set the " +
                        "appropriate value for [ " + XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey() + " ] setting.");
            }
        }
        
        return FIPSCheckResult.success();
    }

    public FIPSCheckResult secureSettingsCheck(FIPSContext context, Environment environment) {
        if (XPackSettings.FIPS_MODE_ENABLED.get(context.settings)) {
            try (KeyStoreWrapper secureSettings = KeyStoreWrapper.load(environment.configFile())) {
                if (secureSettings != null && secureSettings.getFormatVersion() < 3) {
                    return FIPSCheckResult.failure("Secure settings store is not of the latest version. Please use " +
                        "bin/elasticsearch-keystore create to generate a new secure settings store and migrate the secure settings there.");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        
        return FIPSCheckResult.success();
    }
}