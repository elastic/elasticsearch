package org.elasticsearch.river.log4j;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.net.SocketNode;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.RootLogger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

public class Log4JRiver extends AbstractRiverComponent implements River {

	private final ThreadPool threadPool;

    private final Client client;

    private final String indexName;

    private final String typeName;

    private final String logConfigFilePath;
   
    private HashMap<InetAddress, LoggerRepository> loggerHierarchyMap;
    
    private LoggerRepository genericHierarchy;
    
    private final int socketPort;
    
    public static String GENERIC = "generic";

    public static String CONFIG_FILE_EXT = ".lcf";
    
    public static final String MSG_PROP_DATE = "date";
    public static final String MSG_PROP_LOGGER = "server";
    public static final String MSG_PROP_THREAD = "thread";
    public static final String MSG_PROP_LEVEL = "level";
    public static final String MSG_PROP_MESSAGE = "message";
    
	@Inject public Log4JRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
		super(riverName, settings);
		this.threadPool = threadPool;
		this.client = client;
		
		if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "log-message");
        } else {
            indexName = riverName.name();
            typeName = "log-message";
        }
		
		if(settings.settings().containsKey("log-config")) {
			Map<String, Object> logConfigSettings = (Map<String, Object>) settings.settings().get("log-config");
			logConfigFilePath = XContentMapValues.nodeStringValue(logConfigSettings.get("config-file-path"), ".");
			socketPort = XContentMapValues.nodeIntegerValue(logConfigSettings.get("listen-port"), 2020);
		} else {
			logConfigFilePath = ".";
			socketPort = 2020;
		}
		
		this.loggerHierarchyMap = new HashMap<InetAddress, LoggerRepository>();
	}
	
	@Override
	public void close() {
	}

	@Override
	public void start() {
        logger.info("starting log4j stream");
        try {
        	String mapping = XContentFactory.jsonBuilder()
        		.startObject()
	        		.startObject(typeName)
	        			.startObject("properties")
	        				.startObject(MSG_PROP_DATE)
	        					.field("type", "date")
	        					.field("format", "MM-dd-YYYY HH:mm:ss.SSS")
	        				.endObject()
	        				.startObject(MSG_PROP_LOGGER)
	        					.field("type", "string")
	        				.endObject()
	        				.startObject(MSG_PROP_THREAD)
	        					.field("type", "string")
	        				.endObject()
	        				.startObject(MSG_PROP_LEVEL)
	        					.field("type", "string")
	        				.endObject()
	        				.startObject(MSG_PROP_MESSAGE)
	        					.field("type", "string")
	        				.endObject()
	        			.endObject()
	        		.endObject()
	        	.endObject()
	        	.string();
        	
            client.admin().indices().prepareCreate(indexName).addMapping(typeName, mapping).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }
        
        try {
        	SocketAcceptor acceptor = new SocketAcceptor();
        	threadPool.cached().execute(acceptor);
        } catch (IOException ioe) {
        	
        }
	}
	
	private LoggerRepository configureHierarchy(InetAddress inetAddress) {
		String s = inetAddress.toString();
		int i = s.indexOf("/");
		if (i == -1) {
			logger.info("Could not parse the inetAddress [" + inetAddress + "]. Using default hierarchy.");
			return genericHierarchy();
		}
		String key = s.substring(0, i);

		File configFile = new File(logConfigFilePath, key + CONFIG_FILE_EXT);
		if (configFile.exists()) {
			RootLogger rootLogger = new RootLogger(Level.ALL);
					
			Hierarchy h = new Hierarchy(rootLogger);
			rootLogger.addAppender(new ESAppender());
			
			this.loggerHierarchyMap.put(inetAddress, h);

			new PropertyConfigurator().doConfigure(configFile.getAbsolutePath(), h);
			return h;
		}
		logger.info("Could not find config file [" + configFile + "].");
		return genericHierarchy();
	}

	private LoggerRepository genericHierarchy() {
		if (this.genericHierarchy == null) {
			RootLogger rootLogger = new RootLogger(Level.ALL);
			this.genericHierarchy = new Hierarchy(rootLogger);
			rootLogger.addAppender(new ESAppender());
			
			File f = new File(logConfigFilePath, GENERIC + CONFIG_FILE_EXT);
			if (f.exists()) {
				new PropertyConfigurator().doConfigure(f.getAbsolutePath(), this.genericHierarchy);
			} else {
				logger.info("Could not find config file [" + f + "]. All received log events will be indexed.");
			}
		}
		return this.genericHierarchy;
	}
	
	private class SocketAcceptor implements Runnable {

		private ServerSocket serverSocket;
		
		public SocketAcceptor() throws IOException {
			serverSocket = new ServerSocket(Log4JRiver.this.socketPort);
		}
		
		@Override
		public void run() {
			try {
				Socket socket = serverSocket.accept();
				
				InetAddress inetAddress = socket.getInetAddress();
				
				LoggerRepository h = (LoggerRepository)loggerHierarchyMap.get(inetAddress);
				if (h == null) {
					h = configureHierarchy(inetAddress);
				}
				SocketNode socketNode = new SocketNode(socket, h);
				
				Log4JRiver.this.threadPool.cached().execute(socketNode);
			} catch (IOException ioe) {
				logger.error("Erroring connecting remote logger", ioe);
			}
		}
	}
	
	private class ESAppender extends AppenderSkeleton implements Appender {
		
		@Override
		protected void append(LoggingEvent loggingEvent) {
			if (logger.isTraceEnabled()) {
                logger.trace("logmessage {} : {}", loggingEvent.getLoggerName(), loggingEvent.getRenderedMessage());
            }
            
			try {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                builder.field(MSG_PROP_DATE, loggingEvent.getTimeStamp());
                builder.field(MSG_PROP_LOGGER, loggingEvent.getLoggerName());
                builder.field(MSG_PROP_THREAD, loggingEvent.getThreadName());
                builder.field(MSG_PROP_LEVEL, loggingEvent.getLevel());
                builder.field(MSG_PROP_MESSAGE, loggingEvent.getRenderedMessage());
                builder.endObject();
                
                IndexRequest request = 
                	Requests.indexRequest(indexName).type(typeName)
                		.create(true).source(builder);
                
                client.index(request, new ActionListener<IndexResponse>() {
					
					@Override
					public void onResponse(IndexResponse response) {
						logger.trace("logmessage indexed");
					}
					
					@Override
					public void onFailure(Throwable throwable) {
						if (ExceptionsHelper.unwrapCause(throwable) instanceof RejectedExecutionException ||
							ExceptionsHelper.unwrapCause(throwable) instanceof NodeClosedException) {
			                // probably the index cluster is shutting down or otherwise not ready
							// so log as debug
							logger.debug("failed to index logmessage since node is unavailable - you can probably ignore this if it occurs during node shutdown or startup", throwable);
			            } else {
			            	// something else is happening, logs as error
			            	logger.error("failed to index logmessage", throwable);
			            }
					}
				});
                		
            } catch (Exception e) {
                logger.warn("failed to construct index request", e);
            }
            
            
		}
		
		@Override
		public boolean requiresLayout() {
			return false;
		}
		
		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
	}
}
