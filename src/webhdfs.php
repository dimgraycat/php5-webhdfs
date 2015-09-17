<?php
/**
 * WebHDFS Class
 *
 * @package WebHDFS
 * @author  Shigemasa Akiyama
 */

include_once __DIR__.'/webhdfs/request.php';

use WebHDFS\Request as WebHDFS_Request;

class WebHDFS {

    /* @const webhdfs format */
    const WEBHDFS_URL_FORMAT = 'http://%s:%d/webhdfs/v1%s?%s';

    /* @var string connection host */
    private $_host;

    /* @var string connection port */
    private $_port;

    /* @var string connection user */
    private $_user;

    public function __construct($host, $port, $user) {
        $this->_port = $port;
        $this->_user = $user;
        $this->_host = $port;
    }

    /**
     * Create and Write to a File
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#CREATE
     *
     * @param string    $path   HDFSファイルパス
     * @param string    $body   データ
     * @param array     $opt    オプション
     * @return boolean
     */
    public function put($path, $body, $opt=null) {
        $opt['op'] = 'CREATE';
        $code = $this->_request('field', 'PUT', $path, $opt, $body);
        return ($code == 201);
    }

    /**
     * Append to a File
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#APPEND
     *
     * @param string    $path       HDFSファイルパス
     * @param string    $body       データ
     * @param integer   $buffersize サイズ
     * @return boolean
     */
    public function append($path, $body, $buffersize=null) {
        $buffersize = empty($buffersize) ? strlen($body) : $buffersize;
        $code = $this->_request(
            'field', 'POST', $path,
            array('op' => 'APPEND', 'buffersize' => $buffersize)
        );
        return ($code == 200);
    }

    /**
     * Open and Read a File
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#OPEN
     *
     * @param string    $path   HDFSファイルパス
     * @param array     $opt    オプション
     * @return mixed
     */
    public function cat($path, $opt=null) {
        $opt['op'] = 'OPEN';
        return $this->_request('buff', 'GET', $path, $opt);
    }

    /**
     * Make a Directory
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#MKDIRS
     *
     * @param string    $path       HDFSファイルパス
     * @param integer   $permission パーミッション
     * @return boolean
     */
    public function mkdir($path, $permission=755) {
        $result = $this->_request(
            'json', 'PUT', $path,
            array('op' => 'MKDIRS', 'permission' => $permission)
        );
        return !empty($result['boolean']);
    }

    /**
     * Delete a File/Directory
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#RENAME
     *
     * @param string $path HDFSファイルパス
     * @param string $dest 変更先
     * @return boolean
     */
    public function mv($path, $dest) {
        $result = $this->_request(
            'json', 'PUT', $path,
            array('op' => 'RENAME', 'destination' => $dest)
        );
        return !empty($result['boolean']);
    }

    /**
     * Delete a File/Directory
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#DELETE
     *
     * @param string    $path       HDFSファイルパス
     * @param boolean   $recursive  再帰フラグ
     * @return boolean
     */
    public function rm($path, $recursive=false) {
        $result = $this->_request(
            'json', 'DELETE', $path,
            array(
                'op'        => 'DELETE',
                'recursive' => ($recursive) ? 'true' : 'false',
            )
        );
        return !empty($result['boolean']);
    }

    /**
     * Status of a File/Directory
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#GETFILESTATUS
     *
     * @param string $path HDFSファイルパス
     * @return mixed
     */
    public function stat($path) {
        return $this->_request(
            'json', 'GET', $path,
            array('op' => 'GETFILESTATUS')
        );
    }

    /**
     * List a Directory
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#LISTSTATUS
     *
     * @param string $path HDFSファイルパス
     * @return mixed
     */
    public function ls($path) {
        return $this->_request(
            'json', 'GET', $path,
            array('op' => 'LISTSTATUS')
        );
    }

    /**
     * Get Home Directory
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#GETHOMEDIRECTORY
     *
     * @return string
     */
    public function homedir() {
        $result = $this->_request(
            'json', 'GET', '',
            array('op' => 'GETHOMEDIRECTORY')
        );
        return empty($result['Path']) ? '' : $result['Path'];
    }

    /**
     * Set Owner
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#SETOWNER
     *
     * @param string $path  HDFSファイルパス
     * @param string $owner オーナー名
     * @param string $group グループ名
     * @return @boolean
     */
    public function chown($path, $owner, $group=null) {
        $opt = array('op' => 'SETOWNER', 'owner' => $owner );
        if (!empty($group)) { $opt['group'] = $group; }
        $code = $this->_request('code', 'PUT', $path, $opt);
        return ($code == 200);
    }

    /**
     * Set Permission
     * @link http://hadoop.apache.org/docs/r1.2.1/webhdfs.html#SETPERMISSION
     *
     * @param string $path          HDFSファイルパス
     * @param string $permission    パーミッション
     * @return @boolean
     */
    public function chmod($path, $mode) {
        $code = $this->_request(
            'code', 'PUT', $path,
            array('op' => 'SETPERMISSION', 'permission' => $mode)
        );
        return ($code == 200);
    }

    /**
     * curl リクエスト
     *
     * @param string $method    指定メソッド
     * @param string $request   HTTPメソッド
     * @param string $path      パス
     * @param array  $params    引数
     * @param string $buff      バッファ
     * @return mixed
     */
    private function _request($method, $request, $path, $params, $buff=null) {

        if($this->_user) {
            $params['user.name'] = $this->_user;
        }

        $url = sprintf(
            self::WEBHDFS_URL_FORMAT,
            $this->_host, $this->_port,
            $path,
            http_build_query($params)
        );
        return WebHDFS_Request::send($url, $method, $request, $buff);
    }
}
