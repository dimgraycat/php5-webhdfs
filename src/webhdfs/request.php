<?php
/**
 * Request関数群
 *
 * @package WebHDFS\Request
 * @author  Shigemasa Akiyama
 */

class WebHDFS_Request {

    /**
     * cURLのリクエスト発行
     *
     * @param string $url       URL
     * @param string $method    指定メソッド
     * @param string $request   HTTPメソッド
     * @param string $buff      バッファ
     * @return mixed
     */
    public static function send($url, $method, $request, $buff=null) {
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl, CURLOPT_CUSTOMREQUEST, $request);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl, CURLOPT_CONNECTTIMEOUT, 4);
        $result = self::$method($curl, $buff);
        curl_close($curl);

        return $result;
    }

    /**
     * データ取得
     *
     * @param resource $curl cURLリソース
     * @param string   $buff ダミー
     * @return string データ
     */
    private static function buff(&$curl, $buff) {
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, true);
        $buff = curl_exec($curl);
        $code = curl_getinfo($curl, CURLINFO_HTTP_CODE);
        if ($code != 200) {
            return null;
        }
        return $buff;
    }

    /**
     * データ送信
     *
     * @param resource $curl cURLリソース
     * @param string   $buff バッファ
     * @return integer HTTPステータスコード
     */
    private static function field(&$curl, $buff) {
        curl_setopt($curl, CURLOPT_HEADER, true);
        $header = curl_exec($curl);
        $code = curl_getinfo($curl, CURLINFO_HTTP_CODE);

        if ($code != 307) { return 400; }

        $matches = array();
        preg_match('/Location:(.*)\n/', $header, $matches);
        $location_url = trim($matches[1]);

        curl_setopt($curl, CURLOPT_URL, $location_url);
        curl_setopt($curl, CURLOPT_INFILESIZE, strlen($buff));
        curl_setopt($curl, CURLOPT_HTTPHEADER, array('Content-Type: application/octet-stream'));
        curl_setopt($curl, CURLOPT_POST, true);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $buff);

        $html = curl_exec($curl);

        return curl_getinfo($curl, CURLINFO_HTTP_CODE);
    }

    /**
     * JSON取得
     *
     * @param resource $curl cURLリソース
     * @param string   $buff バッファ
     * @return string JSONデータ
     */
    private static function json(&$curl, $buff) {
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, true);
        $json = curl_exec($curl);
        $code = curl_getinfo($curl, CURLINFO_HTTP_CODE);

        if ($code != 200) { return 400; }
        return json_decode($json, true);
    }

    /**
     * PUT処理
     *
     * @param resource $curl cURLリソース
     * @param string   $buff バッファ
     * @return string HTTPステータスコード
     */
    private static function code(&$curl, $buff) {
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, true);
        $json = curl_exec($curl);
        return curl_getinfo($curl, CURLINFO_HTTP_CODE);
    }
}
