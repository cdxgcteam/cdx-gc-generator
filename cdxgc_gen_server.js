#!/usr/bin/env node
// ----------------------------
// Info:
// ----------------------------
// Title: cdxgc_gen_server.js
// Description: CDX Grey Cell Tasking Generator Server
// Author: Derek Yap <zangzi@gmail.com>
// License: MIT
// Version: 0.0.2
var version = '0.0.2';

// ----------------------------
// Requires:
// ----------------------------
// - Built-ins:
var util = require('util');
var fs = require('fs');
var crypto = require('crypto');
var os = require('os');

// - Underscore/Lodash
//var _ = require('underscore');
var _ = require('lodash-node');

// - Commander (Command Line Utility)
var cdxgc_gen_args = require('commander');

// - Random number generators:
var randgen = require('random-seed');
var mainrand = null;
var wellprng = require('well-rng');
var well = new wellprng();

// - Redis -> Redis Driver/Adapter
var redis = require('redis');
var redisclient_msg = null;
var redisclient = null;

// - AMQP -> RabbitMQ Connection Library
var amqp = require('amqplib');
var coreChannel = null;

// - CSV
var csvparser = require('csv');

// - Promises...
var when = require('when');
//var q = require('q');

// - Logging
var winston = require('winston');
var logger = new (winston.Logger)({
	exitOnError: false,
	transports: [
		new (winston.transports.Console)({level: 'debug', colorize: true, timestamp: true})//,
		//new (winston.transports.File)({ filename: 'info.log' })
	]//,
	// exceptionHandlers: [
	// 	new winston.transports.File({ filename: 'exceptions.log' })
	// ]
 });

// ----------------------------
// GLOBALS:
// ----------------------------
var SOURCE_SYSTEM_NAME = 'cdxgenerator';
var AMQP_EXCHANGE = 'topic_cdxtasks';
var BASE_CERT_PATH = '/home/cdxgcserver/cdx_gc_certs2';
var AMQP_OPTS = {
	cert: fs.readFileSync(BASE_CERT_PATH + '/generator/cert.pem'),
	key: fs.readFileSync(BASE_CERT_PATH + '/generator/key.pem'),
	// cert and key or
	// pfx: fs.readFileSync('../etc/client/keycert.p12'),
	//passphrase: 'kJppRZYkdm4Fc5xr',
	ca: [fs.readFileSync(BASE_CERT_PATH + '/rmqca/cacert.pem')]
};
var AMQP_PORT = 5671;

var REDIS_PORT = 6379;
var REDIS_HOST = '127.0.0.1';
var REDIS_SENT_HEADER = 'sent_meta_';
var REDIS_SENT_ORDER_KEY = 'cdx_sent_order';
var REDIS_MAL_HEADER = 'mal_meta_';
var REDIS_MAL_ORDER_KEY = 'cdx_mal_order';

// var REDIS_CMD_SUBSCRIPTION = 'redis_cmd_sub';
// var REDIS_MAL_SUBSCRIPTION = 'redis_mal_sub';

var REDIS_MAL_QUEUE_KEY = 'cdx_mal_submits_queue';
var REDIS_CMD_QUEUE_KEY = 'cdx_cmd_submits_queue';
var REDIS_MAL_LOCK_KEY = 'cdx_mal_lock_key';
var REDIS_CMD_LOCK_KEY = 'cdx_cmd_lock_key';

var POSSIBLE_LARGE_SEEDS = [
'80Ez$1q{t/<obTqS!.(bNpW%%jZE{,v~zBtachaB$axT4Y*;,~Q2XfK_O@x5lr1<F%;nK@CxChsB*;9sGA3g(xlZ4d|K%4n9oxGYQzcp0_z$6]A8qk,hH33(c%!AqpxxQ<Em%oZSp{xBNS2u3z(PO]x=/-KQ$R()%h_R5eaV%EMUsoVg^3Y?xqUV6{%c0ea>]k@dJ8W},E~i7H]n3cOHg$)aS%:K.Z$ar$I-ii4eHlVS&YWsZA[O639arx_FLuvz',

'o|>E{BLC/_#p))/x<Od3hK7h45V=vxO>^Cp%IvEx|YI?A;@:?u~gK~!f;k>^5XbW;U;{ji3jt5#.R,XyZ(.;!0?B,fnM|/Lkb],]<P{Ah>s3KAPW2^X*/uYg#cES_@}cQ>Mt^U_x@{+u7xCG[/%Pk,o.b*+}9AD&Lx)SeWabBg~pwB?]B/qbWZ&cYoeMx*_%bxk_?toV+,7h]g^k_x3sx+ahT@Lphx}~,x3xH336VkBAxjA_eu=*7N3+kMlonD-h',

'}IOt1_xENR0@oJsD0AG|EE(jKn&4<EJ=p[a[Y*is0Qx.jv)pg_=4@-Z$o)$=23x.S~w;WG}wX3jx#P2RuoR-=gxaMW&&}QOk!sii5=+sz?w3?LI98>*f3ljhfT3|~[Ag<jEMmxF!#iW&IYFO^@27nz}G/~T*Ex3U3t,Yxh8b/>K3U|)MQ-/6T.heLhkJjeg3{u*s7(4y5#(0mqo<Js7Qn}y|4E{TK!;{kk5,9LxxD@VtSvE&5W8OoyrrxY@iGP>',

'%O,61ZoH.hE$}K$5C?w,N(d+vqF0@)P/%;cHVSm@53B[0PKcw^u/K961=U-06ZtFEsxY*4H#@3c+]Vw}P}l9U+o}wu:K.C:=}R9E~Bcmu)X6t+:phnW}/xJG)3^1Fqdt:+~Ddog>|hT8%1LTdtj^N)szs42kx.!xGi}8[%^4?E~-QzxL3|AI,]1xOC{L<Cs+b~u4Md>[xxfZAd8r)[Srj#OI8@!X(gB^}dR=)VxRQ(XsM9adF?_z:E-.33H<7RH',

'xre{}K$O#Z~o$wel68ZU/ow?!y-erZS&&um%fI%aHyTk](UVZIxgwcQ/(b/sviXA5wQ5A8rL-I#lS4b)1V}xhw7%{8h3xs$4P_Pc)Wp<WCyhp<TxjGPY<[eJ8;:7UAxC/g$Q5kVEwV551IX*~qBXuxcQ5gKx1(LOJa~8O)aYMYOE,TgI;(D!i[0~_ohr{@~b[WzC[R;bK4A9~l5}E%@opa{]S1-Z+reMnksvK3](o_uqb3/]x%XLz~c>2&RNm{-,mx'
];
var CHOOSEN_SEED = 0;
var UNDERGRAD_SCHOOLS = ['all','navy', 'marines', 'army', 'airforce', 'coastguard', 'nps', 'rmc'];
var COMMANDS_AVAILABLE = ['execute_url','pause', 'quit'];
var GRAD_SCHOOLS = ['nps'];
var MIN_TIME = 200; // in milliseconds
//var MAX_TIME_MIN = 5; // 5 minutes
//var MAX_TIME = 300000; // 5 mins = 300000 milliseconds
var MAX_TIME_MIN = 3; // 3 minutes
var MAX_TIME = MAX_TIME_MIN*1000*60; // 3 mins = 180000 milliseconds
//var TASK_GEN_CYCLE_TIME = 500; // in milliseconds == .5 seconds
var TASK_GEN_CYCLE_TIME = 30000; // in milliseconds == 30 seconds
var MAL_MAX_THRESHOLD = 100;
var MAL_DEFAULT_THRESHOLD = 30;
var URL_PREFACE = 'http://';
var URL_LIST = [];
var CYCLE_TIMER = null;
var COUNTERS = {
	execute_url_cnt: 0,
	pause_cnt: 0,
};

// ----------------------------
// Commander:
// ----------------------------

cdxgc_gen_args
	.version(version)
	.option('-ah, --amqp_host <server name or IP>', 'AMQP Server Host', os.hostname())
	.option('-ap, --amqp_port <port number>', 'AMQP Server Port', AMQP_PORT)
	.option('-rh, --redis_host <server name or IP>', 'Redis Server Host', REDIS_HOST)
	.option('-rp, --redis_port <port number>', 'Redis Server Port', REDIS_PORT)
	.option('-i, --inputFile <file>', 'Input file of URL\'s available to send')
	.option('-u, --urlPreface <format>', 'Add URL Preface', URL_PREFACE)
	.option('-c, --csvFormat <format>', 'Specify CSV Header Format')
	.option('-th, --threshold <number>', 'Specify a threshold to allow a malicious task. Threshold must be <= '+MAL_MAX_THRESHOLD+'. (Default: '+MAL_DEFAULT_THRESHOLD+')', MAL_DEFAULT_THRESHOLD)
	.option('-r, --doRandomTest', 'Print out 100 Random Counts and Times')
	.option('-xt, --maxTime <time: ms>', 'Set Maximum Time in milliseconds a single job will last for. Default: '+MAX_TIME_MIN+' mins = '+MAX_TIME+' ms.', MAX_TIME)
	.option('-mt, --minTime <time: ms>', 'Set Minimum Time in milliseconds a single job will last for. Default: '+MIN_TIME+' ms. If randomly selected time is less then minimum time then minimum time is added to the randomly selected time.', MIN_TIME)
	.option('-ct, --cycleTime <time: ms>', 'Set Time in milliseconds to generate a new task. Default: '+TASK_GEN_CYCLE_TIME+' ms.', TASK_GEN_CYCLE_TIME)
	.parse(process.argv);

// ----------------------------
// Core Functions:
// ----------------------------

var start = function() {
	// 		console.log(util.inspect(cdxgc_gen_args.inputFile, {color: true, showHidden: true, depth: null }));
	var deferred = when.defer();
	
	logger.info('Starting CDX GC Job Generator');
	
	// Error out if we don't get an input file.
	if (!_.isString(cdxgc_gen_args.inputFile)) {
		logger.error('Input File is REQUIRED! Please specify an input file using the \'-i <file>\' or \'--inputFile <file>\'.');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.minTime <= 0) {
		logger.error('Minimum Time specified is less than or equal to 0. Specify a minimum time greater than 0!');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.maxTime <= 0) {
		logger.error('Maximum Time specified is less than or equal to 0. Specify a maximum time greater than 0!');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.maxTime <= cdxgc_gen_args.minTime) {
		logger.error('Maximum Time is less than or equal to Minimum time. Specify a minimum time less than maximum time or a maximum time that is greater than the minimum time!');
		process.exit(1); // May need to kick out.
	}
	
	if (cdxgc_gen_args.cycleTime < 1000) {
		logger.error('Task cycle time must be at least 1000 milliseconds. Please specify a number equal or greater than 1000.');
		process.exit(1); // May need to kick out.
	}
	
	logger.info('AMQP Server: ' + cdxgc_gen_args.amqp_host);
	logger.info('AMQP Port: ' + cdxgc_gen_args.amqp_port);
	logger.info('Redis Server: ' + cdxgc_gen_args.redis_host);
	logger.info('Redis Port: ' + cdxgc_gen_args.redis_port);
	logger.info('Input File Path: ' + cdxgc_gen_args.inputFile);
	logger.info('URL Preface: ' + cdxgc_gen_args.urlPreface);
	logger.info('CSV Format: ' + cdxgc_gen_args.csvFormat);
	logger.info('Max Time: ' + cdxgc_gen_args.maxTime);
	logger.info('Min Time: ' + cdxgc_gen_args.minTime);
	logger.info('Cycle Time: ' + cdxgc_gen_args.cycleTime);

	// Resolver:
	deferred.resolve(cdxgc_gen_args.inputFile);
	
	// Redis Setup:
	redisclient = redis.createClient(cdxgc_gen_args.redis_port, cdxgc_gen_args.redis_host);
	redisclient.on('ready', function() {
		logger.info('Redis :: Main Redis Connection Ready.');
	});
    redisclient.on('error', function (err) {
        logger.error('Redis Error :: ' + err);
    });
	// redisclient_msg.on('ready', function() {
	// 	logger.info('Redis :: Reciever Redis Connection Ready.');
	// });
	// redisclient_msg.on('message', redisCmdRecieve);
	// redisclient_msg.subscribe(REDIS_CMD_SUBSCRIPTION);
	
	return deferred.promise;
};

var csvparse = function(inputFile) { // CSV Pars
	var deferred = when.defer();
	
	var urlPreface = null;
	var csvFormat = true;
	if (_.isString(cdxgc_gen_args.urlPreface)) {
		urlPreface = cdxgc_gen_args.urlPreface;
		logger.debug('csvparse :: URL Preface: ' + urlPreface);
	}
	if (_.isString(cdxgc_gen_args.csvFormat)) {
		csvFormat = cdxgc_gen_args.csvFormat.split(',');
		logger.debug('csvparse :: CSV Header Format Specified: ' + csvFormat);
	}
	
	csvparser()
		.from(inputFile, {columns: csvFormat}) // Read in the input list.
		.transform(function (row, index) {
			if(row.URL.toLowerCase() == 'url') {
				return null;
			}
			
			if(!_.isNull(urlPreface)) {
				row.URL = urlPreface + row.URL;
			}
			return row;
		})
		.to.array(function(data, count) {
			logger.debug('csvparse :: Input Count: ' + count);
			deferred.resolve(data);
		});
		
	return deferred.promise;
};

// var redisCmdRecieve = function (channel, message) {
// 	logger.debug('redisCmdRecieve :: channel: ' + channel + ' :: msg: ' + message);
// };

var mainTaskExecutor = function () {
	// Step 1; See if there is a general command:
	var genCmdPromise = getGeneralCommand();
	genCmdPromise.then(function (genCmd) {
		logger.info('mainTaskExecutor :: GenCmd : ' + util.inspect(genCmd));
		if (!_.isBoolean(genCmd)) { // If there is a general command, run it:
			//logger.info('mainTaskExecutor :: GenCmd : ' + util.inspect(genCmd));
			
			//Execute Command:
			var genCmdWorkObj = generateTasking(genCmd, genCmd.cmd);
			submitWork(REDIS_SENT_HEADER, genCmdWorkObj, 'all', 'task');
		
		} else {
			//Step 2: Check to see if there is task available.
			//Step 3: If there is a task set then choose a random number from 0 to 100.
			//        Anything below 30 will be executed or a random URL will be chosen.
		
			var malCmdPromise = getMalCommand(cdxgc_gen_args.threshold);
			malCmdPromise.then(function (malCmd) {
				logger.info('mainTaskExecutor :: MalCmd : ' + util.inspect(malCmd));
				if (!_.isBoolean(malCmd)) {
					//logger.info('mainTaskExecutor :: MalCmd : ' + util.inspect(malCmd));

					//Execute Command:
					var malCmdWorkObj = generateTasking(malCmd, 'execute_url');
					submitWork(REDIS_MAL_HEADER, malCmdWorkObj, 'all', 'task');
				} else {
					logger.info('mainTaskExecutor :: Random Command...');
			
					var randomWork = {};
					randomWork.url = getRandomURL(URL_LIST);
					var randCmdWorkObj = generateTasking(randomWork, 'execute_url');
					logger.info('mainTaskExecutor :: randomWork :\n' + util.inspect(randCmdWorkObj));
					submitWork(REDIS_SENT_HEADER, randCmdWorkObj, 'all', 'task');
				}
			});
		}
	});
};


// ----------------------------
// Utils:
// ----------------------------

var redisLock = {
	lockKeyHash: null,
	intervalCounter: 0,
	intervalObj: null,
	
	// lockKey : Redis Key to lock against
	// maxTime : In milliseconds, the expire time for the key
	setLock: function (lockKey, maxTime, maxRetries, maxTimeBetweenTries) {
		logger.debug('redisLock :: setLock :: Input :: lockKey: '+lockKey+' maxTime: '+maxTime+' maxRetries: '+maxRetries+' maxTimeBetweenTries: ', maxTimeBetweenTries);

		// Setup promise:
		var lockDefer = when.defer();
		var lockDeferRes = lockDefer.resolver;
		
		// Build lock string:
		var currentDate = new Date();
		var currentDigest = currentDate.toJSON() + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED];
		var shasum = crypto.createHash('sha1');
		shasum.update(currentDigest, 'utf8');
		var hashout = shasum.digest('hex');
		
		// Build set command:
		var curMaxRetries = maxRetries || 5;
		var curMaxTimeBetweenTries = maxTimeBetweenTries || 50;
		var setArgs = [lockKey, hashout, 'PX', maxTime, 'NX'];
		logger.debug('redisLock :: setLock :: Max Retries: '+curMaxRetries+' Max Time Between Tries: '+curMaxTimeBetweenTries);
		logger.debug('redisLock :: setLock :: Set Command Input: '+util.inspect(setArgs));
		redisLock.intervalObj = setInterval(function () {
			redisclient.set(setArgs, function (err, reply) {
				logger.debug('redisLock :: setLock :: Reply: '+reply+' Err: '+err);
				if(reply === 'OK') {
					redisLock.lockKeyHash = hashout;
					lockDeferRes.resolve(hashout);
					clearInterval(redisLock.intervalObj);
				} else {
					redisLock.intervalCounter++;
					if(redisLock.intervalCounter == curMaxRetries) {
						clearInterval(redisLock.intervalObj);
						lockDeferRes.reject('lockFail');
					}
				}
			});
		}, curMaxTimeBetweenTries);
		
		return lockDefer.promise;
	},
	releaseLock: function (lockKey, lockHash) {
		logger.debug('redisLock :: releaseLock :: lockKey: '+lockKey+' lockHash: '+lockHash);
		// release lock Script:
		var releaseLuaCode = 'if redis.call("get",KEYS[1]) == ARGV[1]\nthen\n    return redis.call("del",KEYS[1])\nelse\n    return 0\nend';
		
		// release lock defer:
		var lockDefer = when.defer();
		var lockDeferRes = lockDefer.resolver;
		
		// Use the hash provided unless one isn't and then take what we have on file in the object:
		var curLockHash = null;
		if (_.isUndefined(lockHash)) {
			curLockHash = redisLock.lockKeyHash;
		} else {
			curLockHash = lockHash;
		}
		var evalArgs = [releaseLuaCode, 1, lockKey, curLockHash];
		logger.debug('redisLock :: releaseLock :: Eval Command Input: '+util.inspect(evalArgs));
		redisclient.eval(evalArgs, function (err, reply) {
			logger.debug('redisLock :: releaseLock :: Reply: '+reply+' Err: '+err);
			if (reply == 1){
				logger.debug('redisLock :: releaseLock :: released!');
				lockDeferRes.resolve(reply);
			} else {
				logger.debug('redisLock :: releaseLock :: failed!');
				lockDeferRes.reject(err);
			}
		});
		return lockDefer.promise;
	}
};

var getMalCommand = function (threshold) {
	var getMalCmd = when.promise(function (resolve, reject, notify) {
		redisclient.llen(REDIS_MAL_QUEUE_KEY, function (err, reply) {
			if (err) {
				reject(err);
			} else {
				resolve(reply);
			}
		});
	}).then(function (keyLen) {
		if(keyLen > 0) {
			
			// Do a random number check to see if there is a malicious
			var randomNumberCheck = _.parseInt(mainrand(MAL_MAX_THRESHOLD));
			//var randomNumberCheck = 20;
			logger.debug('getMalCommand :: Random Number :: Selected: '+randomNumberCheck+' Threshold: '+threshold);
			if (randomNumberCheck <= threshold) {
				var curLockKey = REDIS_MAL_LOCK_KEY;
				var popedObj = null;
				// Handle all the command output:
				var lockProm = redisLock.setLock(curLockKey, 1000);
				return lockProm.then(function (lockHash) {
					logger.debug('getMalCommand :: lockHash: '+lockHash);
					var listPopDefer = when.defer();
					var listPopDeferRes = listPopDefer.resolver;
					redisclient.lpop(REDIS_MAL_QUEUE_KEY, function (err, reply) {
						if (_.isString(reply)) {
							listPopDeferRes.resolve({
								lock: lockHash,
								replyStr: reply
							});
						} else {
							listPopDeferRes.reject(err);
						}
					});
		
					return listPopDefer.promise;
				}).then(function (popedInfo) {
					logger.debug('PopedInfo: '+util.inspect(popedInfo));
				
					popedObj = JSON.parse(popedInfo.replyStr);
					logger.debug('PopedObj: '+util.inspect(popedObj));
				
					// Release the lock:
					redisLock.releaseLock(curLockKey,popedInfo.lock);
					return popedObj;
				});
			} else {
				return false;
			}
		} else {
			return false;
		}
	});
	return getMalCmd;
};

var getGeneralCommand = function () {
	var getGenCmd = when.promise(function (resolve, reject, notify) {
		redisclient.llen(REDIS_CMD_QUEUE_KEY, function (err, reply) {
			if (err) {
				reject(err);
			} else {
				resolve(reply);
			}
		});
	}).then(function (keyLen) {
		if(keyLen > 0) {
			var curLockKey = REDIS_CMD_LOCK_KEY;
			var popedObj = null;
			// Handle all the command output:
			var lockProm = redisLock.setLock(curLockKey, 1000);
			return lockProm.then(function (lockHash) {
				logger.debug('getGeneralCommand :: lockHash: '+lockHash);
				var listPopDefer = when.defer();
				var listPopDeferRes = listPopDefer.resolver;
				redisclient.lpop(REDIS_CMD_QUEUE_KEY, function (err, reply) {
					if (_.isString(reply)) {
						listPopDeferRes.resolve({
							lock: lockHash,
							replyStr: reply
						});
					} else {
						listPopDeferRes.reject(err);
					}
				});
		
				return listPopDefer.promise;
			}).then(function (popedInfo) {
				logger.debug('PopedInfo: '+util.inspect(popedInfo));
				
				popedObj = JSON.parse(popedInfo.replyStr);
				logger.debug('PopedObj: '+util.inspect(popedObj));
				
				// Release the lock:
				redisLock.releaseLock(curLockKey,popedInfo.lock);
				return popedObj;
			});
		} else {
			return false;
		}
	});
	return getGenCmd;
};

var submitWork = function(taskType, taskObj, school, secondaryPath) {
	// NOTE: taskType == Redis Header.
	var curOrderKey = null;
	if (taskType == REDIS_SENT_HEADER) {
		curOrderKey = REDIS_SENT_ORDER_KEY;
	} else if (taskType == REDIS_MAL_HEADER) {
		curOrderKey = REDIS_MAL_ORDER_KEY;
	}
	
	var currentKey = getRoutingKey(school, secondaryPath);
	logger.info('Routing Key: ' + currentKey);
	
	// Convert the taskObj to a string for submittal to clients.
	var tarkObjStr = JSON.stringify(taskObj);
	tarkObjStr = tarkObjStr.replace(/,\"poc\":\"\w+\",/gi, ",");
	
	// Add the info to Redis server:
	var fullKey = taskType + taskObj.taskid;
	logger.info('submitWork :: Sent Key: ' + fullKey);
	//var hmsetArgs = [fullKey, taskObj];
	redisclient.hmset(fullKey, taskObj, function(err, reply) {
		logger.debug('submitWork :: hmset :: Err: '+err+' Reply: '+util.inspect(reply));
	});
	logger.info('submitWork :: Put in sort order: ' + fullKey + ' :: Time(ms): ' + taskObj.taskCreateMS);

	// Set in sent key order:
	var zaddArgs = [curOrderKey, taskObj.taskCreateMS, fullKey];
	logger.debug('submitWork :: zaddArgs :: '+util.inspect(zaddArgs));
	redisclient.zadd(zaddArgs, function (err, reply) {
		logger.debug('submitWork :: zadd :: Err: '+err+' Reply: '+util.inspect(reply));
	});

	coreChannel.publish(AMQP_EXCHANGE, currentKey, new Buffer(tarkObjStr));
	logger.info('submitWork :: Exchange: '+AMQP_EXCHANGE+' :: Sent %s:"%s"', currentKey, tarkObjStr);
};

var getRandomURL = function (inputList) {
	// Get a Random URL:
	var URL_pick = _.parseInt(mainrand(inputList.length));
	logger.debug('getRandomURL :: URL Pick: ' + URL_pick);
	return inputList[URL_pick].URL;
};

var getWorkTime = function (minTime, maxTime) {
	// Ensure the minTime number is always a number or set to a default one:
	if (_.isFinite(minTime)) {
		minTime = _.parseInt(minTime);
	} else {
		minTime = MIN_TIME;
	}
	var currentTime = _.parseInt(mainrand(maxTime));
	if (currentTime < minTime) {
		logger.debug('getWorkTime :: Min Time Hit - Adding MIN_TIME (' + minTime + ') to value ('+ currentTime +').');
		currentTime += parseInt(minTime);
	}
	logger.debug('getWorkTime :: Time Pick: ' + currentTime);
	return currentTime;
}

// var getURLandWorkTime = function(inputList) {
// 	
// 	var URL_pick = mainrand(inputList.length);
// 	logger.debug('getURLandWorkTime :: URL Pick: ' + URL_pick);
// 	var currentTime = _.parseInt(mainrand(cdxgc_gen_args.maxTime));
// 	if (currentTime < cdxgc_gen_args.minTime) {
// 		logger.debug('getURLandWorkTime :: Min Time Hit - Adding MIN_TIME (' + cdxgc_gen_args.minTime + ') to value ('+ currentTime +').');
// 		currentTime += parseInt(cdxgc_gen_args.minTime);
// 	}
// 	logger.debug('Time Pick: ' + currentTime);
// 	
// 	return new Array(URL_pick, currentTime);
// };

var generateTaskID = function (taskObjStr) {
	// Current Date:
	var currentDate = new Date();
	// Hash:
	var currentDigestInput = taskObjStr + currentDate.toJSON() + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED];
	var shasum = crypto.createHash('sha1');
	shasum.update(currentDigestInput, 'utf8');
	var hashout = shasum.digest('hex');
	
	logger.debug('generateTaskID :: TaskID CurDate: ' + currentDate.toJSON());
	logger.debug('generateTaskID :: TaskID Input: ' + currentDigestInput);
	logger.debug('generateTaskID :: TaskID SHA1: ' + hashout);

	var outputObj = {
		curDate: currentDate.toJSON(),
		curDateMS: currentDate.getTime(),
		taskID: hashout
	};
	
	return outputObj;
};

// var getTaskID = function(workArray) {
// 	
// 	var currentDate = new Date();
// 	var currentDigest = workArray.toString() + currentDate.toJSON() + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED];
// 	var shasum = crypto.createHash('sha1');
// 	shasum.update(currentDigest, 'utf8');
// 	var hashout = shasum.digest('hex');
// 	logger.debug('getTaskID :: TaskID CurDate: ' + currentDate.toJSON());
// 	logger.debug('getTaskID :: TaskID Input: ' + currentDigest);
// 	logger.debug('getTaskID :: TaskID SHA1: ' + hashout);
// 	
// 	return new Array(currentDate.toJSON(), hashout, currentDate.getTime());
// };

var getRoutingKey = function (school, secondaryPath) {
	var out = null;
	var choosenSchool = _.indexOf(UNDERGRAD_SCHOOLS, school);
	if ( choosenSchool > -1) {
		out = UNDERGRAD_SCHOOLS[choosenSchool];
		if (!_.isString(secondaryPath)) {
			out += '.task';
		} else {
			out += '.' + secondaryPath;
		}
	} else {
		logger.warn('getRoutingKey :: Invalid School Specified: '+school+' :: This shouldn\'t happen...');
	}
	return out;
};

var generateTasking = function(workObj, commandStr) {
	var msgStr = null;
	
	logger.debug('generateTasking :: workObj: '+util.inspect(workObj));
	
	var choosenCommand = _.indexOf(COMMANDS_AVAILABLE,commandStr);
	if (choosenCommand == -1) {
		logger.warn('Invalid Command Specified: '+commandStr+' :: This shouldn\'t happen...');
		return msgStr;
	}
	
	// Get String Version JSON String:
	var workObjJSONStr = JSON.stringify(workObj);
	logger.debug('generateTasking :: workObjJSONStr: '+workObjJSONStr);
	
	// Generate Task ID:
	var taskIDObj = generateTaskID(workObjJSONStr);
	logger.debug('generateTasking :: taskIDObj:\n'+util.inspect(taskIDObj));
	
	// Get Work Time:
	var curMinTime = cdxgc_gen_args.minTime;
	if (!_.isUndefined(workObj.minTime)) {
		curMinTime = workObj.minTime;
	}
	// Ensure the curMinTime number is always a number or set to a default one:
	if (_.isFinite(curMinTime)) {
		curMinTime = _.parseInt(curMinTime);
	} else {
		curMinTime = cdxgc_gen_args.minTime;
	}
	logger.debug('generateTasking :: curMinTime: '+util.inspect(curMinTime));

	var workTime = getWorkTime(curMinTime, cdxgc_gen_args.maxTime);
	// For Pause Commands...
	if (!_.isUndefined(workObj.pauseTimeMS)) {
		workTime = workObj.pauseTimeMS;
	}
	logger.debug('generateTasking :: workTime: '+util.inspect(workTime));
	
	// Build the message:
	var msg_to_send = {};
	msg_to_send.srcSystem = SOURCE_SYSTEM_NAME;					//Source System Generator
	msg_to_send.poc = SOURCE_SYSTEM_NAME;						//POC == Source System Generator
	if (!_.isUndefined(workObj.poc)) {
		msg_to_send.poc = workObj.poc;
	}
	
	msg_to_send.cmd = COMMANDS_AVAILABLE[choosenCommand];		//Choosen Command
	msg_to_send.taskCreateDate = taskIDObj.curDate;				//Task Creation Date/Time
	msg_to_send.taskCreateMS = taskIDObj.curDateMS;				//Task Creation Date/Time in Milliseconds.
	msg_to_send.taskid = taskIDObj.taskID;						//Task ID
	msg_to_send.url = workObj.url;								//URL Itself
	msg_to_send.minWorkTime = curMinTime;						//Min Work Time in MS
	msg_to_send.workTime = workTime;							//How long to sit and wait on page == work time.
	
	logger.debug('generateTasking :: msg_to_send:\n'+util.inspect(msg_to_send));
	
	return msg_to_send;
};

// var getTasking = function (input, commandStr) {
// 	
// 	var msgStr = null;
// 	
// 	var choosenCommand = _.indexOf(COMMANDS_AVAILABLE,commandStr);
// 	if (choosenCommand == -1) {
// 		logger.warn('Invalid Command Specified: '+commandStr+' :: This shouldn\'t happen...');
// 		return msgStr;
// 	}
// 	
// 	var URLandWorkTime = getURLandWorkTime(urlListArray);
// 	logger.info('getTasking :: URL,WorkTime: ' + URLandWorkTime);
// 
// 	//Task ID:
// 	var currentTaskIDInfo = getTaskID(URLandWorkTime);
// 	logger.info('getTasking :: Task Creation Date: ' + currentTaskIDInfo[0]);
// 	logger.info('getTasking :: Task ID: ' + currentTaskIDInfo[1]);
// 
// 	// Put message together:
// 	var msg_to_send = {};
// 	msg_to_send.srcSystem = SOURCE_SYSTEM_NAME;					//Source System Generator
// 	msg_to_send.poc = SOURCE_SYSTEM_NAME;						//POC == Source System Generator
// 	msg_to_send.cmd = COMMANDS_AVAILABLE[choosenCommand];		//Choosen Command
// 	msg_to_send.taskCreateDate = currentTaskIDInfo[0];			//Task Creation Date/Time
// 	msg_to_send.taskCreateMS = currentTaskIDInfo[2];			//Task Creation Date/Time in Milliseconds.
// 	msg_to_send.taskid = currentTaskIDInfo[1];					//Task ID
// 	msg_to_send.urlNum = urlListArray[URLandWorkTime[0]].Rank;	//URL Number/Rank
// 	msg_to_send.url = urlListArray[URLandWorkTime[0]].URL;		//URL Itself
// 	msg_to_send.minWorkTime = cdxgc_gen_args.minTime;			//Min Work Time in MS
// 	msg_to_send.workTime = URLandWorkTime[1];					//How long to sit and wait on page == work time.
// 
// 	// Prepare message for sending:
// 	msgStr = JSON.stringify(msg_to_send);
// 	
// 	return new Array(msgStr, msg_to_send);
// };

var printCounters = function (counters) {
	logger.info('Counters:\n' + util.inspect(counters, {color: true, showHidden: true, depth: null }))
};

// ----------------------------
// Runner:
// ----------------------------

start()
.then(csvparse)
.then(function (inputList) { // Start Random Setup:
	
	// Setup InputList to be generally available:
	URL_LIST = inputList;
	
	logger.silly(util.inspect(URL_LIST, {color: true, showHidden: true, depth: null }));
	logger.debug('InputList Length: ' + URL_LIST.length);
	
	//Random Setup:
	CHOOSEN_SEED = well.randInt(0, (POSSIBLE_LARGE_SEEDS.length-1));
	logger.info('Choosen Seed [' + CHOOSEN_SEED + ']: ' + POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED]);
	
	mainrand = randgen.create(POSSIBLE_LARGE_SEEDS[CHOOSEN_SEED]);
	
	// Random Test:
	if (cdxgc_gen_args.doRandomTest) {
		for (var i=0;i<100;i++) {
			logger.info('-->Pick '+ i +': Start');
			var URLandWorkTime = getURLandWorkTime(URL_LIST);
			logger.debug('URL,WorkTime: ' + URLandWorkTime);

			//Task ID:
			var currentTaskID = getTaskID(URLandWorkTime);
			logger.info('Task ID: ' + currentTaskID);
			logger.debug('-->Pick '+ i +': Stop');
		}
	}


	//return amqpServer;
})
.then(function () {
	
	// Connect AMQP:
	var amqpServer = amqp.connect('amqps://' + cdxgc_gen_args.amqp_host + ':' + cdxgc_gen_args.amqp_port, AMQP_OPTS);
	
	amqpServer.then(function (amqpConn) {
		// Setup signals:
		process.on('SIGINT', function () {
			logger.info('SIGNAL: SIGINT caught: Closing connection.');
			clearInterval(CYCLE_TIMER);
			amqpConn.close();
			printCounters(COUNTERS);
			process.exit(1); // May need to kick out.
		});
		process.on('SIGTERM', function () {
			logger.info('SIGNAL: SIGTERM caught: Closing connection.');
			clearInterval(CYCLE_TIMER);
			amqpConn.close();
			printCounters(COUNTERS);
			process.exit(1); // May need to kick out.
		});
		
		return amqpConn.createChannel().then(function(ch) {
			coreChannel = ch;
			var ok = ch.assertExchange(AMQP_EXCHANGE, 'topic', {durable: false});
			return ok.then(function() {
				CYCLE_TIMER = setInterval(mainTaskExecutor,cdxgc_gen_args.cycleTime);
			});
		});
		
	}).then(null,function (err) {
		logger.error('AMQP Error :: '+ err);
	});
});

