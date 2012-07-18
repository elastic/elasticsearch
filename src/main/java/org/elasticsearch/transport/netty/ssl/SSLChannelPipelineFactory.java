package org.elasticsearch.transport.netty.ssl;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.jboss.netty.channel.ChannelPipelineFactory;

/**
 * ChannelPipelineFactory used for Client/Server SSL channel pipelines
 * 
 * @author Tanguy Leroux
 *
 */
public abstract class SSLChannelPipelineFactory implements ChannelPipelineFactory {

	private static final ESLogger logger = Loggers.getLogger(SSLChannelPipelineFactory.class);
	
    private SSLContext sslContext;

    SecureMessageChannelHandler messageChannelHandler;
    
    private String keyStore;
    
    private String keyStorePassword;
    
    private String keyStoreAlgorithm;
    
    private String trustStore;
    
    private String trustStorePassword;
    
    private String trustStoreAlgorithm;

    ByteSizeValue maxCumulationBufferCapacity;
    
    int maxCompositeBufferComponents;

	public SSLChannelPipelineFactory(SecureMessageChannelHandler channelHandler,
			String sslKeyStore, String sslKeyStorePassword, String sslKeyStoreAlgorithm, 
			String sslTrustStore, String sslTrustStorePassword, String sslTrustStoreAlgorithm,
			ByteSizeValue mCumulationBufferCapacity, int mCompositeBufferComponents) {
		
		messageChannelHandler = channelHandler;
		maxCumulationBufferCapacity = mCumulationBufferCapacity;
		maxCompositeBufferComponents = mCompositeBufferComponents;
		
		keyStore = sslKeyStore;
		keyStorePassword = sslKeyStorePassword;
		if (sslKeyStoreAlgorithm != null) {
			keyStoreAlgorithm = sslKeyStoreAlgorithm;
		} else {
			keyStoreAlgorithm =  KeyManagerFactory.getDefaultAlgorithm();
		}
		
		trustStore = sslTrustStore;
		trustStorePassword = sslTrustStorePassword;
		if (sslTrustStoreAlgorithm != null) {
			trustStoreAlgorithm = sslTrustStoreAlgorithm;
		} else {
			trustStoreAlgorithm =  TrustManagerFactory.getDefaultAlgorithm();
		}

        logger.debug("using keyStore[{}], keyAlgorithm[{}], trustStore[{}], trustAlgorithm[{}]", keyStore, keyStoreAlgorithm, trustStore, trustStoreAlgorithm);
        
        KeyStore ks = null;
        KeyManagerFactory kmf = null;
        FileInputStream in = null;
        try {
            // Load KeyStore
            ks = KeyStore.getInstance("jks");
            in = new FileInputStream(keyStore);
            ks.load(in, keyStorePassword.toCharArray());

            // Initialize KeyManagerFactory
            kmf = KeyManagerFactory.getInstance(keyStoreAlgorithm);
            kmf.init(ks, keyStorePassword.toCharArray());
        } catch (Exception e) {
            throw new Error("Failed to initialize a KeyManagerFactory", e);
        } finally {
            try {
                in.close();
            } catch (Exception e2) {
            }
        }
        
        TrustManager[] trustManagers = null;
        try {
            // Load TrustStore
            in = new FileInputStream(trustStore);
            ks.load(in, trustStorePassword.toCharArray());

            // Initialize a trust manager factory with the trusted store
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);    
            trustFactory.init(ks);

            // Retrieve the trust managers from the factory
            trustManagers = trustFactory.getTrustManagers();
        } catch (Exception e) {
            throw new Error("Failed to initialize a TrustManagerFactory", e);
        } finally {
            try {
                in.close();
            } catch (Exception e2) {
            }
        }
        
        // Initialize sslContext
        try {
        	sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), trustManagers, null);
		} catch (Exception e) {
			throw new Error("Failed to initialize the SSLContext", e);
		}
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }
}
