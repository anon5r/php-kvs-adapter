<?php
/**
 * Simplify and basicaly operate interface adapter abstract class
 * for various key value store
 *
 * $kvs = My_KeyValueStore::factory( $configs );
 *
 * [ SET the value for key name ]
 * If you want to set $value to key name of <KeyName>
 *  and result by boolean.
 * $value  = mixed value.
 * $exp    = OPTIONAL; expire time  by seconds (integer). default is 0 ( disabled expire).
 * $result = boolean, when success to set returns true, or when failed returns false.
 *
 * $result = $kvs->set<KeyName>( $value, $exp );
 *
 *
 * [ GET the value by key name ]
 * If you want to get $value from key name of <KeyName>
 * $value = mixed value when you set before, or failed, returns false.
 *
 * $value = $kvs->get<KeyName>();
 *
 * If you specify the first parameter, you can chose value in <KeyName> values.
 * however this option only enabled when the value of array.
 *
 *
 *
 * [ APPEND the value for key name ]
 * If you want to apped $value to existing key name of <KeyName>, You can append
 * the value specified it.
 *
 * $value  = mixed value.
 * $exp    = OPTIONAL; expire time  by seconds (integer). default is 0 ( disabled expire).
 * $result = boolean, when success to set returns true, or when failed returns false.
 *
 *
 * [ REMOVE the specified index of values into key name ]
 * If you want to remove the specified index of values into existing key name of <KeyName>.
 *
 * $index  = index key of values.
 * $result = boolean, when success to set returns true, or when failed returns false.
 *
 * $result $kvs->remove<KeyName>( $index );
 *
 *
 *
 * [ PULL the specified index of values into key name ]
 * If you want to pull ( to get and remove the value ) the specified index of values into existing key name of <KeyName>.
 *
 * $index  = index key of values.
 * $value  = mixed value when you set before, or failed, returns false.
 *
 * $result $kvs->pull<KeyName>( $index );
 *
 *
 * [ INCREMENT the value to key name ]
 * If you want increment value to existing key name of <KeyName>.
 *
 * $index  = index key of values.
 * $result = boolean, when success to set returns true, or when failed returns false.
 *
 * $result $kvs->increment<KeyName>( $index );
 *
 *
 *
 * [ DECREMENT the value from key name ]
 * If you want decrement from existing key name of <KeyName>.
 *
 * $value  = decremented value
 *
 * $result $kvs->decrement<KeyName>( $index );
 *
 * anon <anon@anoncom.net>
 */
abstract class My_KeyValueStore_Adapter_Abstract {
	
	/**
	 * Method prefix lists
	 * @var array
	 */
	protected static $_methodPrefixes = array(
		'set',			// Set or overwrite value to the key
		'get',			// Get value from the key
		'append',		// Append to value to the values into the key
		'remove',		// Remove the value having specific index from the key
		'pull',			// Pull the value having specific index from the key
		'fetch',		// Fetch the values within a given index from the key
		'fetchAll',		// Fetch all values from the key (Same as "get")
		'increment',	// Increment value to the key
		'decrement',	// Decrement value from the key 
		'drop',			// Drop the key and value
	);
	
	/**
	 * Method arguments vars name
	 * @var array
	 */
	protected static $_argumentNames = array(
		'set'		=> array( 'value', 'expiration', ),
		'get'		=> array( 'index' ),
		'append'	=> array( 'value', 'expiration' ),
		'remove'	=> array( 'index' ),
		'pull'		=> array( 'index' ),
		'fethch'	=> array( 'fetch', 'offset' ),
		'increment'	=> array( 'offset' ),
		'decrement'	=> array( 'offset' ),
	);
	
	
	
	/**
	 * Set keyword to prepend for operation key name
	 * @var array
	 */
	protected $_keyPrefix = array();
	
	/**
	 * Set delimiter character to concatenates for the operation prefix  key name
	 * @var char
	 */
	protected $_keyPrefixDelimiter = '-';
	
	/**
	 * Set keyword to append for operation key name
	 * @var array
	 */
	protected $_keySuffix = array();
	
	/**
	 * Set delimiter character to concatenates for the operation suffix  key name
	 * @var char
	 */
	protected $_keySuffixDelimiter = '-';
	
	/**
	 * Connection hostname
	 * @var string
	 */
	protected $_host = '';
	
	/**
	 * Connection port number
	 * @var int
	 */
	protected $_port = 0;
	
	/**
	 * Timeout seconds
	 * @var int
	 */
	protected $_timeout = 10;
	
	
	/**
	 * Allow only to use specified keys
	 * @var array
	 */
	protected $_allowedKeys = array();
	
	/**
	 * Connection instances pool
	 * @var array
	 */
	protected static $_pool = array();
	
	/**
	 * Connection object
	 * @var object
	 */
	protected static $_connection = null;
	
	/**
	 * Constructor
	 *
	 * $config is an array of key/value pairs or an instance of Zend_Config
	 * containing configuration options.  These options are common to most adapters:
	 *
	 * host           => (string) What host to connect to, defaults to localhost
	 * port           => (int) The port of the database
	 * timeout        => (int) Connection timed out seconds
	 *
	 * Some options are used on a case-by-case basis by adapters:
	 *
	 * username       => (string) Connect to the database as this username.
	 * password       => (string) Password associated with the username.
	 * persistent     => (boolean) Whether to use a persistent connection or not, defaults to false
	 * protocol       => (string) The network protocol, defaults to TCPIP
	 * caseFolding    => (int) style of case-alteration used for identifiers
	 *
	 * @param array $config An array having configuration data
	 * @throws My_KeyValueStore_Exception
	 */
	public function __construct( array $config ) {
		
		$this->_checkExtension();
		
		if ( isset( $config[ 'host' ] ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Configuration array must have a key for "host" that names the key value store instance.' );
		}
		if ( isset( $config[ 'port' ] ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Configuration array must have a key for "port" that names the key value store instance.' );
		} elseif ( is_numeric( $config[ 'port' ] ) == false || ( $config[ 'port' ] < 0 ) || $config[ 'port' ] > 65535 ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Invalid value specified in configuration array a key names "timeout".' );
		}
		
		
		$this->_host = $config[ 'host' ];
		$this->_port = $config[ 'port' ];
		
		if ( isset( $config[ 'timeout' ] ) == true ) {
			$this->_timeout = intval( $config[ 'timeout' ] );
		}
		
	}
	
	/**
	 * Check using extension
	 * @return bool
	 */
	abstract protected function _checkExtension();
	
	/**
	 * Creates a connection to the key value store.
	 *
	 * @return void
	 */
	abstract protected function _connect();
	
	
	
	/**
	 * Add allowed key to list
	 * @param mixed $key
	 * @return My_KeyValueStore
	 */
	public function _addAllowKey( $key ) {
		$this->_allowedKeys[] = $key;
		return $this;
	}
	
	/**
	 * Check the key allowed specified keys
	 * @param string $key
	 * @return bool
	 */
	protected function _isAllowed( $key ) {
		if ( count( $this->_allowedKeys ) > 0 ) {
			return isset( $this->_allowdKeys[ $key ] );
		}
		return true;
	}
	
	/**
	 * Set allowed keys list
	 * @param array $keys
	 * @return My_KeyValueStore
	 */
	public function _setAllowKeys( array $keys ) {
		$this->_allowedKeys = $keys;
		return $this;
	}
	
	/**
	 * Set the delimiter character to concatenates a prefix appended to the key name
	 * @param char $delimiter
	 * @return My_KeyValueStore
	 */
	public function _setKeyPrefixDelimiter( $delimiter ) {
		$this->_keyPrefixDelimiter = $delimiter;
		return $this;
	}
	
	/**
	 * Sets the suffix keyword to set the operation key name
	 * @param int $index set position
	 * @param string $keyword set keyword
	 * @return My_KeyValueStore
	 */
	public function _setKeyPrefix( $index, $keyword ) {
		$this->_keyPrefix[ $index ] = $keyword;
		return $this;
	}
	
	/**
	 * Append the suffix keyword to prepend the operation key name
	 * @param string $keyword
	 * @return My_KeyValueStore
	 */
	public function _appendKeyPrefix( $keyword ) {
		$this->_keyPrefix[] = $keyword;
		return $this;
	}
	
	/**
	 * Delete the prefix keyword from appended the operation key name
	 * @param int $index set position
	 * @return My_KeyValueStore
	 */
	public function _removeKeyPrefix( $index ) {
		unset( $this->_keyPrefix[ $index ] );
		return $this;
	}
	
	/**
	 * Clear the suffix keyword to append the operation key name
	 * @return My_KeyValueStore
	 */
	public function _clearKeyPrefix() {
		$this->_keyPrefix = array();
		return $this;
	}
	
	/**
	 * Set the delimiter character to concatenates a suffix appended to the key name
	 * @param char $delimiter
	 * @return My_KeyValueStore
	 */
	public function _setKeySuffixDelimiter( $delimiter ) {
		$this->_keySuffixDelimiter = $delimiter;
		return $this;
	}
	
	/**
	 * Sets the suffix keyword to append the operation key name
	 * @param int $index set position
	 * @param string $keyword set keyword
	 * @return My_KeyValueStore
	 */
	public function _setKeySuffix( $index, $keyword ) {
		$this->_keySuffix[ $index ] = $keyword;
		return $this;
	}
	
	/**
	 * Append the $keyword keyword to append the operation key name
	 * @param string $keyword
	 * @return My_KeyValueStore
	 */
	public function _appendKeySuffix( $keyword ) {
		$this->_keySuffix[] = $keyword;
		return $this;
	}
	
	/**
	 * Delete the suffix keyword from appended the operation key name
	 * @param int $index
	 * @return My_KeyValueStore
	 */
	public function _removeKeySuffix( $index ) {
		unset( $this->_keySuffix[ $index ] );
		return $this;
	}
	
	/**
	 * Clear the suffix keyword to append the operation key name
	 * @return My_KeyValueStore
	 */
	public function _clearKeySuffix() {
		$this->_keySuffix = array();
		return $this;
	}
	
	/**
	 * KeyValue control method
	 * @param string $name method name
	 * @param array $arguments method arguments
	 * @return Ambiguous
	 * @throws My_KeyValueStore_Exception
	 */
	public function __call( $name, $arguments ) {
	
		foreach ( self::$_methodPrefixes as $prefix ) {
			if ( strpos( $name, $prefix, 0 ) === 0 ) {
				$keyName = substr( $name, strlen( $prefix ) );
				
				if ( $this->_isAllowed( $keyName ) == false ) {
					require_once 'My/KeyValueStore/Exception.php';
					throw new My_KeyValueStore_Exception( 'Specified key "' . $keyName . '" does not allowed on this class.', My_KeyValueStore_Exception::CODE_KEY_NOT_ALLOWED_KEY );
				}
				
				if ( is_array( $this->_keyPrefix ) && count( $this->_keyPrefix ) > 0 ) {
					// If the suffix key is set, add the suffix key name
					$keyName = implode( $this->_keyPrefixDelimiter, $this->_keyPrefix ) . $this->_keyPrefixDelimiter . $keyName;
				}
				if ( is_array( $this->_keySuffix ) && count( $this->_keySuffix ) > 0 ) {
					// If the suffix key is set, add the suffix key name
					$keyName .= $this->_keySuffixDelimiter . implode( $this->_keySuffixDelimiter, $this->_keySuffix );
				}
				$baseMethod = sprintf( '_%sBase', $prefix );
				return $this->$baseMethod( $keyName, $arguments );
			}
		}
		require_once 'My/KeyValueStore/Exception.php';
		throw new My_KeyValueStore_Exception( 'Undefined method called. ' . $name );
	}
	
	/**
	 * Convert arguments to named base parameters
	 * @param string $type
	 * @param array $arguments
	 * @return array
	 */
	protected static function _convertArguments( $type, array $arguments ) {
		$args = array();
		if ( isset( self::$_argumentNames[ $type ] ) == false ) {
			return $arguments;
		}
		foreach ( self::$_argumentNames[ $type ] as $key => $name ) {
			$args[ $name ] = isset( $arguments[ $key ] ) ? $arguments[ $key ] : null;
			unset( $arguments[ $key ] );
		}
		$arguments = array_values( $arguments );	// 配列を0から再整列する
		return array_merge( $args, $arguments );	// 名前付き配列と残りの値をマージする
	}
	
	/**
	 * Get counter of incremented value from $name + "Count"
	 * @param string $name
	 */
	abstract protected function _getAppendKey( $name );
	
	
	/**
	 * set values for specified key into key value store
	 * @param string $name key name
	 * @param bool
	 */
	abstract protected function _setBase( $name, array $arguments = null );
	
	/**
	 * get values by specified key from key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return mixed
	 */
	abstract protected function _getBase( $name, array $arguments = null );
	
	
	/**
	 * append values into specified key for key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	abstract protected function _appendBase( $name, array $arguments );
	
	/**
	 * remove values into specified key for key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	abstract protected function _removeBase( $name, array $arguments );
	
	
	/**
	 * pull values by specified key for key value store
	 * this method returns pulled values if it succeed.
	 * @param string $name
	 * @param array $arguments
	 * @return Ambiguous<mixed,false>
	 * @throws My_KeyValueStore_Exception
	 */
	abstract protected function _pullBase( $name, array $arguments );
	
	
	/**
	 * fetch values counted by specified fetch count from specified key
	 * @param string $name
	 * @param array $arguments
	 * @return array
	 * @throws My_KeyValueStore_Exception
	 */
	abstract protected function _fetchBase( $name, array $arguments );
	
	/**
	 * fetch all values by specified key
	 * @param string $name
	 * @param array $arguments
	 */
	abstract protected function _fetchAllBase( $name, array $arguments = null );
	
	
	/**
	 * increment value for specified key and return incremented value
	 * @param string $name
	 * @param array $arguments
	 * @return int
	 */
	abstract protected function _incrementBase( $name, array $arguments );
	
	/**
	 * decrement value for specified key and return incremented value
	 * @param string $name
	 * @param array $arguments
	 * @return int
	 */
	abstract protected function _decrementBase( $name, array $arguments = null );
	
	/**
	 * drop key from key value store
	 * @param string $name
	 * @param array $arguments
	 * @return bool
	 */
	abstract protected function _dropBase( $name, array $arguments = null );
	
	
	/**
	 *
	 * Returns the underlying database connection object or resource.
	 * If not presently connected, this initiates the connection.
	 *
	 * @return object|resource|null
	 */
	public function getConnection() {
		$this->_connect();
		return $this->_connection;
	}
}
