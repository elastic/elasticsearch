package org.elasticsearch.cloud.aws.network;

import java.io.IOException;
import java.net.InetAddress;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.elasticsearch.common.network.NetworkService.CustomNameResolver;

/**
 * Resolves certain ec2 related 'meta' hostnames into an actual hostname
 * obtained from ec2 meta-data.
 * <p /> 
 * Valid config values for {@link Ec2HostnameType}s are -
 * <ul>
 *     <li>_ec2 - maps to privateIpv4</li>
 *     <li>_ec2:privateIp_ - maps to privateIpv4</li>
 *     <li>_ec2:privateIpv4_</li>
 *     <li>_ec2:privateDns_</li>
 *     <li>_ec2:publicIp_ - maps to publicIpv4</li>
 *     <li>_ec2:publicIpv4_</li>
 *     <li>_ec2:publicDns_</li>
 * </ul>
 * @author Paul_Loy (keteracel)
 */
public class Ec2NameResolver implements CustomNameResolver {

	// enum that can be added to over time with more meta-data types (such as ipv6 when this is available)
	private static enum Ec2HostnameType {

		PRIVATE_IPv4("_ec2:privateIpv4_", "local-ipv4"),
		PRIVATE_DNS ("_ec2:privateDns_",  "local-hostname"),
		PUBLIC_IPv4 ("_ec2:publicIpv4_",  "public-ipv4"),
		PUBLIC_DNS  ("_ec2:publicDns_",   "public-hostname"),

		PUBLIC_IP   ("_ec2:publicIp_",    PUBLIC_IPv4.ec2Name),
		PRIVATE_IP  ("_ec2:privateIp_",   PRIVATE_IPv4.ec2Name),
		DEFAULT     ("_ec2",              PRIVATE_IPv4.ec2Name);

		final String configName;
		final String ec2Name;

		private Ec2HostnameType(String configName, String ec2Name) {
			this.configName = configName;
			this.ec2Name = ec2Name;
		}

	}

	private static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

	private final Ec2HostnameType type;

	/**
	 * Construct a {@link CustomNameResolver} with the given {@link Ec2HostnameType}
	 * address type.
	 * 
	 * @param addressType the type of ec2 host to bind to.
	 */
	public Ec2NameResolver(Ec2HostnameType addressType) {
		this.type = addressType;
	}

	/**
	 * @return the appropriate host resolved from ec2 meta-data.
	 * @throws IOException if ec2 meta-data cannot be obtained.
	 * 
	 * @see CustomNameResolver#resolve()
	 */
	@Override
	public InetAddress resolve() throws IOException {
		String ec2Url = EC2_METADATA_URL + this.type.ec2Name;
		GetMethod ec2MetadataRequest = new GetMethod(ec2Url);

		int status = new HttpClient().executeMethod(ec2MetadataRequest);
		if (status != 200) {	
			throw new HttpException(MessageFormat.format("unable to retrieve ec2 metadata from {0}. Response: {1} {2}", ec2Url, status, HttpStatus.getStatusText(status)));
		}
		String metadataResult = ec2MetadataRequest.getResponseBodyAsString();
		return InetAddress.getByName(metadataResult);
	}

	public static Map<String, CustomNameResolver> resolvers() {
		Map<String, CustomNameResolver> resolvers = new HashMap<String, CustomNameResolver>();
		for (Ec2HostnameType type : Ec2HostnameType.values()) {
			resolvers.put(type.configName, new Ec2NameResolver(type));
		}
		return resolvers;
	}

}
