
brokers (List of strings)
    **Required**. The list of MQTT brokers to connect to.
    The MQTT client will attempt to connect to each broker in the list, one by one, until a connection is successfully established.
    The brokers can be specified using either the format tcp://host:port for unencrypted connections or ssl://host:port for encrypted connections.
    
username (string) 
	The username to use when connecting to the MQTT broker. If not specified an annonymous connection will be performed.
	Default: not specified 

password (string)
	**Required** when the username is specified. 
	The password to use when connecting to the MQTT broker. If not specified an annonymous connection will be performed.
	Default: not specified

clientId (string)
   The client ID to use for the MQTT connection. This is used by the broker to identify the client. The broker will refuse the connection if another client witht he same clientId is already connected.
   If you are using two links you have to specify a different clientId for each (or just leave the default randomly assigned clientId).
   If not specified, it will be automatically generated using a random value.
	
connectionTimeoutSecs
	The maximum time, in seconds, to wait for a connection to the MQTT broker before timing out.
	Default: 5
	
autoReconnect (boolean)
	If set to true, the client will automatically attempt to reconnect to the broker if the connection is lost.
	Default: true
	
keepAliveSecs (integer)
	The keep-alive interval, in seconds, for the MQTT connection.
	This is the maximum period between communications with the broker before the connection is considered lost.
	Default: 60

mqttVersion (string)
	The MQTT protocol version to use when connecting to the broker. One of ``default``, ``3.1`` or ``3.1.1``.
	With ``default``, the client first tries 3.1.1 and falls back to 3.1 if that fails. In this case the error reported for a failed connection is the one from the 3.1 attempt, which may hide the real cause.
	Default: default
