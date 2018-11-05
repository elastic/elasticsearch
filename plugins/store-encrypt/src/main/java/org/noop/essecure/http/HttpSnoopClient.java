/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */


package org.noop.essecure.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieEncoder;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.File;
import java.net.URI;

public final class HttpSnoopClient {

	public interface HttpListener{

		public void OnSuccess(String url,String content,String debugMessage);
		public void OnFail(String url,String debugMessage);
	};

	static final String URL = "https://wx.qq.com/";

	public static void main(String[] args) throws Exception {
		HttpListener listener = new HttpListener (){
			public void OnSuccess(String url,String content,String debugMessage)
			{
				System.out.println(url + content + debugMessage);
			}
			public void OnFail(String url,String debugMessage)
			{
				System.out.println(url + debugMessage);
			}
		};

		SnoopHttpRequest request = new SnoopHttpRequest(URL,3000,"d:\\g_1.cer");
		submitHttpRequest(request,listener);
	}

	public static void submitHttpRequest(SnoopHttpRequest request,HttpListener listener) throws Exception {

		URI uri = new URI(request.url());
		String scheme = uri.getScheme() == null? "http" : uri.getScheme();
		String host = uri.getHost() == null? "127.0.0.1" : uri.getHost();
		int port = uri.getPort();
		if (port == -1) {
			if ("http".equalsIgnoreCase(scheme)) {
				port = 80;
			} else if ("https".equalsIgnoreCase(scheme)) {
				port = 443;
			}
		}

		if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
			System.err.println("Only HTTP(S) is supported.");
			return;
		}

		// Configure SSL context if necessary.
		final boolean ssl = "https".equalsIgnoreCase(scheme);
		final SslContext sslCtx;
		if (ssl) {
			if(request.rootCAFile()!= null)
			{
				sslCtx = SslContextBuilder.forClient()
						.trustManager(new File(request.rootCAFile())).build();
			}
			else
			{
				sslCtx = SslContextBuilder.forClient()
						.trustManager(InsecureTrustManagerFactory.INSTANCE).build();
			}
		} else {
			sslCtx = null;
		}

		// Configure the client.
		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap();
			b.group(group)
			.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, request.timeoutMS())
			.channel(NioSocketChannel.class)
			.handler(new HttpSnoopClientInitializer(sslCtx,listener,request.timeoutMS()));

			// Make the connection attempt.
			Channel ch = b.connect(host, port).sync().channel();

			// Prepare the HTTP request.
			HttpRequest httpRequest = new DefaultFullHttpRequest(
					HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
			httpRequest.headers().set(HttpHeaderNames.HOST, host);
			httpRequest.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
			httpRequest.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

			// Send the HTTP request.
			ch.writeAndFlush(httpRequest);

			// Wait for the server to close the connection.
			ch.closeFuture().sync();
		} finally {
			// Shut down executor threads to exit.
			group.shutdownGracefully();
		}
	}
}
