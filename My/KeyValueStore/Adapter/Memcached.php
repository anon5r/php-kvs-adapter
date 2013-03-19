<?php
require_once 'My/KeyValueStore/Adapter/Abstract.php';

/**
 * Simplify and basicaly operate interface for Memcached
 * @see http://jp2.php.net/memcached
 * @author anon <anon@anoncom.net>
 */
class My_KeyValueStore_Adapter_Memcached extends My_KeyValueStore_Adapter_Abstract {

	/**
	 * cas token value by each keys
	 * @var array
	 */
	protected $_casTokens = array();

	/**
	 * Check using extension
	 * @return bool
	 */
	protected function _checkExtension() {

		if ( extension_loaded( 'memcached' ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'The Memcached extension is required for this adapter but the extension is not loaded', My_KeyValueStore_Exception::CODE_EXTENSION_UNAVAILABLE );
		}
		if ( class_exists( 'Memcached' ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'PHP Mecached driver does not loaded.', My_KeyValueStore_Exception::CODE_CLASS_NOTEXIST );
		}

		return true;
	}

	/**
	 * Creates a Memcached object and connects to the key value store.
     *
     * @return void
     * @throws My_KeyValueStore_Exception
	 */
	public function _connect() {

		$instanceHash = sprintf( 'memcached://%s:%d', $this->_host, $this->_port );

		// already having instances for this connection
		if ( isset( self::$_pool[ $instanceHash ] ) == true && self::$_pool[ $instanceHash ] instanceof Memcached ) {
			self::$_connection = self::$_pool[ $instanceHash ];
			return;
		}

		self::$_connection = new Memcached;
		self::$_connection->setOption( Memcached::OPT_RECV_TIMEOUT, $this->_timeout );
		self::$_connection->setOption( Memcached::OPT_SEND_TIMEOUT, $this->_timeout );
		//self::$_connection->setOption( Memcached::OPT_POLL_TIMEOUT, $this->_timeout );
		self::$_connection->addServer( $this->_host, $this->_port );
		self::$_pool[ $instanceHash ] = self::$_connection;
	}

	/**
	 * Get counter of incremented value from $name + "Count"
	 * @param string $name
	 */
	protected function _getAppendKey( $name ) {
		$offset = 1;
		return $this->_incrementBase( $name, array( $offset ) );
	}


	/**
	 * set values for specified key into key value store
	 * @param string $name key name
	 * @param array $arguments arguments
	 * @return bool
	 */
	protected function _setBase( $name, array $arguments = null ) {
		extract( self::_convertArguments( 'set', $arguments ) );
		$this->_connect();
		if( isset( $expiration ) ){
			$result = self::$_connection->set( $name, $value, $expiration );
		}
		else{
			$result = self::$_connection->set( $name, $value );
		}
		return $result;
	}

	/**
	 * get values by specified key from key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return mixed
	 */
	protected function _getBase( $name, array $arguments = null ) {
		if ( $arguments != null ) {
			extract( self::_convertArguments( 'get', $arguments ) );
		}
		$this->_connect();
		$values = self::$_connection->get( $name );
		if ( $values === false && self::$_connection->getResultCode() == Memcached::RES_NOTFOUND ) {
			// If it is determined that the determination key is not found
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key name "' . $name . '" does not found on this server "' . $this->_host . ':' . $this->_port . '".', Mys_KeyValueStore_Exception::CODE_KEY_NOT_FOUND, $e );
		}

		if ( $arguments != null && isset( $index ) == true ) {
			// index specified
			if ( isset( $values[ $index ] ) == false ) {
				// specified index does not found
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified index does not found on the key name "' . $name . '"\'s value on this server "' . $this->_host . ':' . $this->_port . '".' );
			}
			$values = $values[ $index ];
		}
		// save cas tokens by key
		$this->_casTokens[ $name ] = $casToken;

		return $values;
	}


	/**
	 * append values into specified key for key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	protected function _appendBase( $name, array $arguments ) {
		extract( self::_convertArguments( 'append', $arguments ) );
		if ( isset( $value ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Appending value does not specified.' );
		}

		$this->_connect();
		$values = $this->_getBase( $name, null );
		if ( $values instanceof ArrayIterator == false && is_array( $values ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key having value is not array or unsupported append method.' );
		}
		if ( $values instanceof ArrayIterator ) {
			$values->append( $value );
		} elseif ( is_array( $values ) == true || $values == null ) {
			if ( method_exists( $this, ( '_getAppendKey' ) ) ) {
				// Call the method _getAppendKey, if implemented, to get the key from there
				$appendKey = $this->_getAppendKey( $name, $userId );
				if ( $appendKey === false || $appendKey == null ) {
					require_once 'My/KeyValueStore/Exception.php';
					throw new My_KeyValueStore_Exception( 'Appending key name does not get.' );
				}
				$values[ $appendKey ] = $value;
			} else {
				// Appended to the current array if it was none
				$values[] = $value;
			}
		}
		// Generate parameter for _setBase() method
		$setArgs = array(
				$values,
				$expiration,
		);
		return $this->_setBase( $name, $setArgs );
	}

	/**
	 * remove values into specified key for key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	protected function _removeBase( $name, array $arguments ) {
		extract( self::_convertArguments( 'remove', $arguments ) );
		if ( isset( $index ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Remove index does not specified.' );
		}

		$this->_connect();
		$values = $this->_getBase( $name, null );
		if ( $values instanceof ArrayIterator == false && is_array( $values ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key having value could not remove by index.' );
		}
		if ( $values instanceof ArrayIterator ) {
			if ( $values->offsetExists( $index ) == false ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified index does not find.' );
			}
			$values->offsetUnset( $index );
		} elseif ( is_array( $values ) == true ) {
			if ( isset( $values[ $index ] ) == false ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified index does not find.' );
			}
			unset( $values[ $index ] );
		}
		// Generate parameter for _setBase() method
		$setArgs = array(
				$values,
				$expiration,
		);
		return $this->_setBase( $name, $setArgs );
	}


	/**
	 * pull values by specified key for key value store
	 * this method returns pulled values if it succeed.
	 * @param string $name
	 * @param array $arguments
	 * @return Ambiguous<mixed,false>
	 * @throws My_KeyValueStore_Exception
	 */
	protected function _pullBase( $name, array $arguments ) {
		extract( self::_convertArguments( 'pull', $arguments ) );
		if ( isset( $index ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Pull index does not specified.' );
		}
		$values = $this->_getBase( $name, null );
		if ( $values instanceof ArrayIterator == false && is_array( $values ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key having value could not remove by index.' );
		}
		if ( $values instanceof ArrayIterator ) {
			if ( $values->offsetExists( $index ) == false ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified index does not find.' );
			}
			$pullValue = $values->offsetGet( $index );
			$values->offsetUnset( $index );
		} elseif ( is_array( $values ) == true ) {
			if ( isset( $values[ $index ] ) == false ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified index does not find.' );
			}
			$pullValue = $values[ $index ];
			unset( $values[ $index ] );
		}
		// Generate parameter for _setBase() method
		$setArgs = array(
				$values,
				$expiration,
		);
		if ( $this->_setBase( $name, $setArgs ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Failed to set values to key' );
		}
		return $pullValue;
	}

	/**
	 * replace values into specified key for key value store
	 * this method returns true if succeeded, else return false.
	 * @param string $name
	 * @param array $arguments
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	public function _replaceBase( $name, array $arguments ) {

		extract( self::_convertArguments( 'replace', $arguments ) );

		if ( isset( $this->_casTokens[ $name ] ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Failed to replace values to key, because the key have not been get this session yet.' );
		}

		$casToken = $this->_casTokens[ $name ];

		$this->_connect();
		if( isset( $expiration ) ){
			$result = self::$_connection->cas( $casToken, $name, $value, $expiration );
		}
		else{
			$result = self::$_connection->cas( $casToken, $name, $value );
		}

		return $result;
	}

	/**
	 * fetch values counted by specified fetch count from specified key
	 * @param string $name
	 * @param array $arguments
	 * @return array
	 * @throws My_KeyValueStore_Exception
	 */
	protected function _fetchBase( $name, array $arguments ) {
		extract( self::_convertArguments( 'pull', $arguments ) );
		if ( isset( $fetch ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Fetch count does not specified.' );
		}
		$fetch = ( int )$fetch;
		if ( isset( $offset ) == false ) {
			$offset = 0;
		}
		$offset = ( int )$offset;
		$fetch = ( $fetch < 0 ) ? 1 : $fetch;

		$values = $this->_getBase( $name, null );
		if ( $values instanceof ArrayIterator == false && is_array( $values ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key having value could not fetch by index.' );
		}

		if ( $values instanceof ArrayIterator ) {
			return array_slice( $values->getArrayCopy(), $offset, $fetch, true );
		} elseif ( is_array( $values ) == true ) {
			return array_slice( $values, $offset, $fetch, true );
		}
	}

	/**
	 * fetch all values by specified key
	 * @param string $name
	 * @param array $arguments
	 */
	protected function _fetchAllBase( $name, array $arguments = null ) {
		$this->_connect();
		return $this->get( $name, null );
	}


	/**
	 * increment value for specified key and return incremented value
	 * @param string $name
	 * @param array $arguments
	 * @return int
	 */
	protected function _incrementBase( $name, array $arguments ) {
		if ( $arguments != null ) {
			extract( self::_convertArguments( 'increment', $arguments ) );
		}
		if ( isset( $offset ) == false ) {
			$offset = 1;
		}
		$counter = 0;

		$this->_connect();
		if ( method_exists( self::$_connection, 'increment' ) ) {
			$counter = self::$_connection->increment( $name, $offset );
			if ( $counter === false ) {
				$counter = 0;
				if ( self::$_connection->getResultCode() == Memcached::RES_NOTFOUND ) {
					$counter = ( 0 + $offset );
					self::$_connection->set( $name, $counter, 0 );
				}
			}
		} else {
			$counter = $this->_getBase( $name, null );
			// FIXME WARNING: signed int型に変換しているため、signed intの幅を超える桁数の場合は、
			// 以降計算されない、あるいは負の数値に変換される可能性があります
			$counter = intval( $counter );
			$counter += $offset;
			$this->_setBase( $name, array( $counter, 0 ) );
		}
		return $counter;
	}

	/**
	 * decrement value for specified key and return incremented value
	 * @param string $name
	 * @param array $arguments
	 * @return int
	 */
	protected function _decrementBase( $name, array $arguments = null ) {
		if ( $arguments != null ) {
			extract( self::_convertArguments( 'decrement', $arguments ) );
		}
		if ( isset( $offset ) == false ) {
			$offset = 1;
		}
		$counter = 0;

		$this->_connect();
		if ( method_exists( self::$_connection, 'decrement' ) ) {
			$counter = self::$_connection->decrement( $name, $offset );
			if ( $counter === false ) {
				$count = 0;
				if ( self::$_connection->getResultCode() == Memcached::RES_NOTFOUND ) {
					$counter = ( 0 - $offset );
					self::$_connection->set( $name, $counter, 0 );
				}
			}
		} else {
			$counter = $this->_getBase( $name, null );
			$counter = intval( $counter );
			$counter -= $offset;
			$this->_setBase( $name, array( $counter, 0 ) );
		}
		return $counter;
	}

	/**
	 * drop key from key value store
	 * @param string $name
	 * @param array $arguments
	 * @return bool
	 */
	protected function _dropBase( $name, array $arguments = null ) {
		$this->_connect();
		return self::$_connection->delete( $name );
	}

	/**
	 * set values for specified key into key value store
	 * @param string $name key name
	 * @param mixed $value
	 * @param bool $compress
	 * @param int $expiration
	 * @param bool
	 */
	public function set( $name, $value, $compress = false, $expiration = null ) {
		$this->_connect();
		// Compress option
		self::$_connection->setOption( Memcached::OPT_COMPRESSION, $compress );

		if( $expiration != null ){
			$result = self::$_connection->set( $name, $value, $expiration );
		}
		else{
			$result = self::$_connection->set( $name, $value );
		}
		return $result;
	}

	/**
	 * get values by specified key from key value store
	 * @param string $name key name
	 * @param array $arguments
	 * @return mixed
	 */
	public function get( $name ) {
		$this->_connect();
		$values = self::$_connection->get( $name, null, $casToken );


		if ( $values === false && self::$_connection->getResultCode() == Memcached::RES_NOTFOUND ) {
			// If it is determined that the determination key is not found
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key name "' . $name . '" does not found on this server "' . $this->_host . ':' . $this->_port . '".', My_KeyValueStore_Exception::CODE_KEY_NOT_FOUND, $e );
		}

		// save cas tokens by key
		$this->_casTokens[ $name ] = $casToken;

		return $values;
	}


	/**
	 * append values into specified key for key value store
	 * @param string $name key name
	 * @param $value
	 * @param int $expiration
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	public function append( $name, $value, $compress = false, $expiration = null ) {
		$this->_connect();
		$values = $this->get( $name );
		if ( $values instanceof ArrayIterator == false && is_array( $values ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Specified key having value is not array or unsupported append method.' );
		}

		if ( $values instanceof ArrayIterator ) {
			$values->append( $value );
		} elseif ( is_array( $values ) == true || $values == null ) {
			if ( method_exists( $this, ( '_getAppendKey' ) ) ) {
				// Call the method _getAppendKey, if implemented, to get the key from there
				$appendKey = $this->_getAppendKey( $name );
				if ( $appendKey === false || $appendKey == null ) {
					require_once 'My/KeyValueStore/Exception.php';
					throw new My_KeyValueStore_Exception( 'Appending key name does not get.' );
				}
				$values[ $appendKey ] = $value;
			} else {
				// Appended to the current array if it was none
				$values[] = $value;
			}
		}
		// Generate parameter for set() method
		return $this->set( $name, $values, $compress, $expiration );
	}


	/**
	 * replace values into specified key for key value store
	 * this method returns true if succeeded, else return false.
	 * @param string $name
	 * @param mixed $value
	 * @param int $expiration
	 * @return bool
	 * @throws My_KeyValueStore_Exception
	 */
	public function replace( $name, $value, $compress = false, $expiration = null ) {
		if ( isset( $this->_casTokens[ $name ] ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Failed to replace values to key, because the key have not been get this session yet.' );
		}

		$casToken = $this->_casTokens[ $name ];

		$this->_connect();
		// Compress option
		self::$_connection->setOption( Memcached::OPT_COMPRESSION, $compress );
		if( $expiration != null ){
			$result = self::$_connection->cas( $casToken, $name, $value, $expiration );
		}
		else{
			$result = self::$_connection->cas( $casToken, $name, $value );
		}

		return $result;
	}


	/**
	 * increment value for specified key and return incremented value
	 * @param string $name
	 * @param int $offset
	 * @return int
	 */
	public function increment( $name, $offset = null ) {
		if ( $offset == null ) {
			$offset = 1;
		}
		$counter = 0;

		$this->_connect();
		if ( method_exists( self::$_connection, 'increment' ) ) {
			$counter = self::$_connection->increment( $name, $offset );
			if ( $counter === false ) {
				$counter = 0;
				if ( self::$_connection->getResultCode() == Memcached::RES_NOTFOUND ) {
					$counter = ( 0 + $offset );
					self::$_connection->set( $name, $counter, 0 );
				}
			}
		} else {
			$counter = $this->get( $name );
			// FIXME WARNING: signed int型に変換しているため、signed intの幅を超える桁数の場合は、
			// 以降計算されない、あるいは負の数値に変換される可能性があります
			$counter = intval( $counter );
			$counter += $offset;
			$this->set( $name, $counter );
		}
		return $counter;
	}

	/**
	 * decrement value for specified key and return incremented value
	 * @param string $name
	 * @param int offset
	 * @return int
	 */
	public function decrement( $name, $offset = null ) {
		if ( $offset == null ) {
			$offset = 1;
		}
		$counter = 0;

		$this->_connect();
		if ( method_exists( self::$_connection, 'decrement' ) ) {
			$counter = self::$_connection->decrement( $name, $offset );
			if ( $counter === false ) {
				$count = 0;
				if ( self::$_connection->getResultCode() == Memcached::RES_NOTFOUND ) {
					$counter = ( 0 - $offset );
					self::$_connection->set( $name, $counter, 0 );
				}
			}
		} else {
			$counter = $this->get( $name, null );
			$counter = intval( $counter );
			$counter -= $offset;
			$this->set( $name, $counter );
		}
		return $counter;
	}

	/**
	 * drop key from key value store
	 * @param string $name
	 * @param array $arguments
	 * @return bool
	 */
	public function delete( $name ) {
		$this->_connect();
		return self::$_connection->delete( $name );
	}

	/**
	 * close connection
	 * connection close supported version from 2.0.0 on Memcached
	 * ignore this call if used other version
	 * @see My_KeyValueStore_Adapter_Abstract::close()
	 */
	public function close() {
		if ( self::$_connection instanceof Memcached && version_compare( phpversion( 'Memcached' ), '2.0.0', '>=' ) ) {
			// connection close supported version from 2.0.0 on Memcached
			self::$_connection->quit();
		}
	}
}
