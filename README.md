# cdx-gc-generator
================

CDX Grey Cell Generator - Generates messages for execution. Each message is a specific task either an executable or a URL.

## Current Server Setup:

Hardware: VMWare, 20 GB storage, 2 GB RAM. (Probably not enough ram but I am going to start with this, it is easy to change.)

OS: Ubuntu 13.10
RabbitMQ Info:

	Status of node rabbit@cdxgcserver ...
	[{pid,xxx},
	 {running_applications,[{rabbit,"RabbitMQ","3.2.1"},
	                        {ssl,"Erlang/OTP SSL application","5.3"},
	                        {public_key,"Public key infrastructure","0.19"},
	                        {crypto,"CRYPTO version 2","3.0"},
	                        {asn1,"The Erlang ASN1 compiler version 2.0.2",
	                              "2.0.2"},
	                        {os_mon,"CPO  CXC 138 46","2.2.12"},
	                        {xmerl,"XML parser","1.3.3"},
	                        {mnesia,"MNESIA  CXC 138 12","4.9"},
	                        {sasl,"SASL  CXC 138 11","2.3.2"},
	                        {stdlib,"ERTS  CXC 138 10","1.19.2"},
	                        {kernel,"ERTS  CXC 138 10","2.16.2"}]},
	 {os,{unix,linux}},
	 {erlang_version,"Erlang R16B01 (erts-5.10.2) [source] [64-bit] [async-threads:30] [kernel-poll:true]\n"},
	 {memory,[{total,37157640},
	          {connection_procs,5264},
	          {queue_procs,5264},
	          {plugins,0},
	          {other_proc,13284944},
	          {mnesia,57856},
	          {mgmt_db,0},
	          {msg_index,33584},
	          {other_ets,782304},
	          {binary,719016},
	          {code,17935183},
	          {atom,703377},
	          {other_system,3630848}]},
	 {vm_memory_high_watermark,0.4},
	 {vm_memory_limit,836829184},
	 {disk_free_limit,50000000},
	 {disk_free,16374022144},
	 {file_descriptors,[{total_limit,924},
	                    {total_used,4},
	                    {sockets_limit,829},
	                    {sockets_used,2}]},
	 {processes,[{limit,1048576},{used,136}]},
	 {run_queue,0},
	 {uptime,7205}]
	...done.

## Software Used:

Node JS - Primary Engine
Key Libraries

- amqplib : For AMQP messaging support.
- winston : For logging.
- commander : For command line management.
- underscore : A utility package. Very handy and helps keep the code cleaner.
- random-seed : This is a psuedo-random number generator.
- well-rng : Anoterh psuedo-random number generator. (Why 2 PNRG's? Cause I can and in case we need to switch them for some reason.)

## Current Commands Available:

	var COMMANDS_AVAILABLE = ["execute_url","pause", "quit", "execute_app"];

- execute_url : Runs a URL and stays on the page for a random amount of time.
- pause : Pause for a period of time before picking up new tasking.
- quit : Quit the clients.
- execute_app : This will download and execute an application. FUTURE ACTIVITY.

## Data Path:

Send work to the clients:

	generator ==> RabbitMQ Server ==> Client 1
								  ==> Client 2
								  ==> ...
	generator ==> RabbitMQ Server ==> TrackingProcessor

Clients respond with work completed:

	Client 1  ==> RabbitMQ Server ==> TrackingProcessor
	Client 2  ==> RabbitMQ Server ==> TrackingProcessor
	Client ...==> RabbitMQ Server ==> TrackingProcessor

TrackingProcessor - The goal is for something to pick up on the work and store it while work is taking place. Still deciding on a storage mechanism because of the amount of data that may be flying around.

## Cert Generation Process

### Make Base Cert Creation Directory:

	# mkdir cdx_gc_certs2
	# cd cdx_gc_certs2/

### Make Main CA Cert:

In base cert directory above:

	# mkdir rmqca
	# cd rmqca/
	# mkdir certs
	# mkdir private
	# chmod 700 private/

Create Cert Index and Serial File:

	# touch index.txt
	# echo 01 > serial

Create openssl.conf file in the same directory with the below:

	[ ca ]
	default_ca = greycellautoca

	[ greycellautoca ]
	dir = .
	certificate = $dir/cacert.pem
	database = $dir/index.txt
	new_certs_dir = $dir/certs
	private_key = $dir/private/cakey.pem
	serial = $dir/serial
	default_crl_days = 7
	default_days = 365
	default_md = sha1
	policy = greycellautoca_policy
	x509_extensions = certificate_extensions

	[ greycellautoca_policy ]
	commonName = supplied
	stateOrProvinceName = optional
	countryName = optional
	emailAddress = optional
	organizationName = optional
	organizationalUnitName = optional

	[ certificate_extensions ]
	basicConstraints = CA:false

	[ req ]
	default_bits = 2048
	default_keyfile = ./private/cakey.pem
	default_md = sha1
	prompt = yes
	distinguished_name = root_ca_distinguished_name
	x509_extensions = root_ca_extensions

	[ root_ca_distinguished_name ]
	commonName = hostname

	[ root_ca_extensions ]
	basicConstraints = CA:true
	keyUsage = keyCertSign, cRLSign

	[ client_ca_extensions ]
	basicConstraints = CA:false
	keyUsage = digitalSignature
	extendedKeyUsage = 1.3.6.1.5.5.7.3.2

	[ server_ca_extensions ]
	basicConstraints = CA:false
	keyUsage = keyEncipherment
	extendedKeyUsage = 1.3.6.1.5.5.7.3.1

Then run the following lines in the shell:

	# openssl req -x509 -config openssl.conf -newkey rsa:4096 -days 365 -out cacert.pem -outform PEM -subj /CN=GREYCELLAUTO/ -nodes
	# openssl x509 -in cacert.pem -out cacert.cer -outform DER

Now build the RabbitMQ Server cert (MAKE SURE TO CD OUT of the rqmca directory):

	# cd ..
	# mkdir server
	# cd server/

Then run the following on the command line:

	# openssl genrsa -out key.pem 4096
	# openssl req -new -key key.pem -out req.pem -outform PEM   -subj /CN=$(hostname)/O=server/ -nodes

Sign the server cert with the CA:

	# cd ../rmqca/
	# openssl ca -config openssl.conf -in ../server/req.pem -out ../server/cert.pem -notext -batch -extensions server_ca_extensions

Now make the generator cert (it simply a client cert):

NOTE: Make sure you cd out of the _rmqca_ directory first!

	# cd ..
	# mkdir generator
	# cd generator/
	# openssl genrsa -out key.pem 4096
	# openssl req -new -key key.pem -out req.pem -outform PEM -subj /CN=$(hostname)/O=generator/ -nodes

Sign the generator cert (Remember to check your paths): 

	# cd ../rmqca/
	# openssl ca -config openssl.conf -in ../generator/req.pem -out ../generator/cert.pem -notext -batch -extensions client_ca_extensions

## Configuring RabbitMQ

See this link: [Rabbit MQ SSL Documentation](http://www.rabbitmq.com/ssl.html)