<?php
require_once 'My/KeyValueStore/Adapter/Abstract.php';

/**
 * Simplify and basicaly operate interface for Redis
 * @see https://github.com/nicolasff/phpredis
 * @author anon <anon@anoncom.net>
 */
class My_KeyValueStore_Adapter_Redis extends My_KeyValueStore_Adapter_Abstract {
	
	/**
	 * Check using extension
	 * @return bool
	 */
	protected function _checkExtension() {
		
		if ( extension_loaded( 'redis' ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'The Redis extension is required for ' . get_class( self ) . ' adapter but the extension is not loaded. Please see following URL: https://github.com/nicolasff/phpredis , and then install it.', My_KeyValueStore_Exception::CODE_EXTENSION_UNAVAILABLE );
		}
		if ( class_exists( 'Redis' ) == false ) {
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( '"Redis" class does not loaded. Please check to been loading it.', My_KeyValueStore_Exception::CODE_CLASS_NOTEXIST );
		}
		
		return true;
	}
	
	/**
	 * Creates a Redis object and connects to the key value store.
     *
     * @return void
     * @throws My_KeyValueStore_Exception
	 */
	protected function _connect() {
		
		$instanceHash = sprintf( 'redis://%s:%d', $this->_host, $this->_port );
		
		if ( isset( self::$_pool[ $instanceHash ] ) == true && self::$_pool[ $instanceHash ] instanceof Redis ) {
			self::$_connection = self::$_pool[ $instanceHash ];
			return;
		}
		
		self::$_connection = new Redis;
		self::$_connection->pconnect( $this->_host, $this->_port, $this->_timeout );
		self::$_connection->setOption( Redis::OPT_SERIALIZER, Redis::SERIALIZER_PHP );
		self::$_pool[ $instanceHash ] = self::$_connection;
	}
	
	/**
	 * Get counter of incremented value from $name + "Count"
	 * @param string $name
	 */
	protected function _getAppendKey( $name ) {
		$offset = 1;
		$this->_connect();
		return $this->_incrementBase( $name, array( $offset ) );
	}
	
	
	/**
	 * set values for specified key into key value store
	 * @param string $name key name
	 * @param bool
	 */
	protected function _setBase( $name, array $arguments = null ) {
		extract( self::_convertArguments( 'set', $arguments ) );
		$this->_connect();
		if ( isset( $expiration ) && $expiration != null && method_exists( $this, 'setex' ) ) {
			return self::$_connection->setex( $name, $expiration, $value );
		}
		return self::$_connection->set( $name, $value );
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
		if ( $arguments != null && isset( $index ) == true ) {
			// index指定がある場合
			if ( isset( $values[ $index ] ) == false ) {
				// 指定されたindexが見つからなかった場合
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified index does not found on the key name "' . $name . '"\'s value' );
			}
			$values = $values[ $index ];
		}
		
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
		
		$result = self::$_connection->rPush( $name, $value );
		if ( $resut == false ) {
			$values = $this->_getBase( $name, null );
			if ( $values instanceof ArrayIterator == false && is_array( $values ) == false ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Specified key having value is not array or unsupported append method.' );
			}
			if ( $values instanceof ArrayIterator ) {
				$values->append( $value );
			} elseif ( is_array( $values ) == true || $values == null ) {
				if ( method_exists( $this, ( '_getAppendKey' ) ) ) {
					// _getAppendKeyというメソッドが実装されていれば、そこからキーを取得する
					$appendKey = $this->_getAppendKey( $name, $userId );
					if ( $appendKey === false || $appendKey == null ) {
						require_once 'My/KeyValueStore/Exception.php';
						throw new My_KeyValueStore_Exception( 'Appending key name does not get.' );
					}
					$values[ $appendKey ] = $value;
				} else {
					// 無ければ現在の配列に追記する
					$values[] = $value;
				}
			}
			// _setBase 実行用パラメータ生成
			$setArgs = array(
				$values,
				$expiration,
			);
			$result = $this->_setBase( $name, $setArgs );
		}
		
		return $result;
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
		// _setBase 実行用パラメータ生成
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
		$pullValue = self::$_connection->lPop( $name );
		
		if ( $result == false ) {
			
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
			// _setBase 実行用パラメータ生成
			$setArgs = array(
				$values,
				$expiration,
			);
			if ( $this->_setBase( $name, $setArgs ) == false ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Failed to set values to key' );
			}
		}
		return $pullValue;
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
		if ( isset( $offset ) == false || $offset == null ) {
			$counter = self::$_connection->incr( $name );
		} else {
			$counter = self::$_connection->incrBy( $name, $offset );
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
		if ( isset( $offset ) ==false || $offset == null ) {
			$counter = self::$_connection->decr( $name );
		} else {
			$counter = self::$_connection->decrBy( $name, $offset );
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
}
