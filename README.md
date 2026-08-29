# Virtual Print Sink

Rust + Slint で作る、macOS / Windows 向けの仮想印刷受信アプリです。

実プリンターの代わりに **LPR (RFC 1179)** または **IPP** で印刷ジョブを受信し、プリンターデータを加工せずファイルとして保存します。GUIからサーバーの開始/停止と保存先の選択ができます。

## できること

- Slint GUI
  - サーバー開始
  - サーバー停止
  - ファイル出力先の選択
  - 出力先パス表示
  - 待受エンドポイント表示
  - 最後に保存したジョブ表示
- LPR受信
  - Receive a printer job
  - control file / data file のACKシーケンス
  - ジョブ名、ユーザー名、送信元などをメタデータとして保存
- IPP受信
  - Print-Job
  - Validate-Job
  - Create-Job / Send-Document
  - Get-Printer-Attributes
  - Get-Job-Attributes / Get-Jobs / Cancel-Job
- 印刷データ本体 + JSONメタデータを保存

## ディレクトリ構成

```text
virtual-print-sink/
├── Cargo.toml
├── build.rs
├── README.md
├── ui/
│   └── app-window.slint
├── src/
│   ├── main.rs
│   ├── storage.rs
│   └── server/
│       ├── mod.rs
│       ├── lpr.rs
│       └── ipp.rs
└── scripts/
    ├── test_ipp.py
    └── test_lpr.py
```

## 必要環境

- Rust stable
- Cargo
- macOS または Windows

Rust未導入の場合は rustup でインストールしてください。

## 起動

```bash
cargo run
```

Releaseビルド:

```bash
cargo build --release
```

初期保存先は、アプリを起動したディレクトリ配下の `print_jobs` です。GUIの「出力先を選択」で変更できます。

## 待受ポート

| OS | LPR | IPP |
|---|---:|---:|
| Windows | `127.0.0.1:515` | `127.0.0.1:8631` |
| macOS | `127.0.0.1:1515` | `127.0.0.1:8631` |

IPPを標準の631番にしていない理由は、macOSでは通常CUPSが631番を使用するためです。

macOSでLPRを1515番にしている理由は、515番が1024未満の特権ポートだからです。Windowsでは通常515番をそのまま利用できます。

ポートは環境変数で変更できます。

```bash
VPS_LPR_PORT=2515 VPS_IPP_PORT=18631 cargo run
```

PowerShell:

```powershell
$env:VPS_LPR_PORT = "2515"
$env:VPS_IPP_PORT = "18631"
cargo run
```

## 保存されるファイル

例:

```text
print_jobs/
├── 20260829_135500_123_IPP_000001_Report.pdf
├── 20260829_135500_123_IPP_000001_Report.json
├── 20260829_135612_456_LPR_000002_TestPage.ps
└── 20260829_135612_456_LPR_000002_TestPage.json
```

本体の拡張子は、IPPの `document-format` またはデータ先頭のマジック値から可能な範囲で判定します。判定できない場合は `.prn` です。

JSONにはプロトコル、受信日時、ジョブ名、ユーザー、LPR control file、IPP属性などを保存します。

## まずプロトコル単体でテストする

サーバーをGUIから開始して、任意ファイルを送信します。

### IPP

```bash
python3 scripts/test_ipp.py sample.pdf --format application/pdf
```

### LPR

macOS:

```bash
python3 scripts/test_lpr.py sample.pdf --port 1515
```

Windows:

```powershell
py scripts\test_lpr.py sample.pdf --port 515
```

## WindowsでIPPプリンターとして登録

アプリを起動し、「サーバー開始」を押してから、管理者PowerShellで以下を試します。

```powershell
Add-Printer -IppURL "http://127.0.0.1:8631/printers/virtual"
```

Windowsの「設定 > Bluetoothとデバイス > プリンターとスキャナー > デバイスの追加 > 手動で追加」からIPPデバイスとして登録する場合も、必要に応じて完全なURLを指定します。

このMVPは一般的なIPP操作と主要属性を実装していますが、IPP Everywhere/Mopriaの認証を受けた実プリンターを完全にエミュレートするものではありません。Windowsのバージョンや保護印刷モードによって、追加時により厳密な能力問い合わせが行われる場合があります。

## WindowsでLPRプリンターとして登録

LPRは標準515番で待ち受けます。

Windowsの「LPD 印刷サービス」ではなく、クライアント側の **LPR Port Monitor** が必要になる場合があります。Windowsの機能でLPR Port Monitorを有効にした後、PowerShellでは概ね次の構成になります。

```powershell
Add-PrinterPort -Name "VirtualPrintSink-LPR" `
  -LprHostAddress "127.0.0.1" `
  -LprQueueName "virtual"
```

その後、使用したいプリンタードライバーと上記ポートを組み合わせてプリンターを追加します。

LPRで保存されるデータ形式は **選択したWindowsプリンタードライバーが生成した形式** です。PostScriptドライバーならPostScript、PCLドライバーならPCLというようになります。

## macOSでIPPプリンターとして登録

サーバー起動後、Terminalから:

```bash
sudo lpadmin \
  -p VirtualPrintSinkIPP \
  -E \
  -v ipp://127.0.0.1:8631/printers/virtual \
  -m everywhere
```

確認:

```bash
lpstat -v VirtualPrintSinkIPP
```

テスト印刷:

```bash
lp -d VirtualPrintSinkIPP sample.pdf
```

`-m everywhere` はサーバーへIPP能力問い合わせを行います。このMVPの属性セットではOS/CUPSバージョンによって登録が拒否される可能性があります。その場合でも `scripts/test_ipp.py` でIPP受信自体を切り分けできます。

## macOSでLPRプリンターとして登録

LPRエンドポイントは次です。

```text
lpd://127.0.0.1:1515/virtual
```

CUPSは `-v` でdevice URIを指定でき、LPD URIにはポートを含められます。利用可能なGeneric PostScriptモデルが存在するか先に確認します。

```bash
lpinfo -m | grep -i "Generic PostScript"
```

OpenPrinting CUPSの標準サンプルドライバーが存在する環境では、例として:

```bash
sudo lpadmin \
  -p VirtualPrintSinkLPR \
  -E \
  -v lpd://127.0.0.1:1515/virtual \
  -m drv:///sample.drv/generic.ppd
```

ただしPPD/従来型ドライバーはCUPSで非推奨化されています。macOSでは **IPP経由を第一候補** にしてください。

## 設計

```text
┌───────────────────────────────┐
│ Slint GUI                     │
│ Start / Stop / Output Folder  │
└───────────────┬───────────────┘
                │
                v
┌───────────────────────────────┐
│ ServerController              │
│ background Tokio runtime      │
└───────────────┬───────────────┘
                │
        ┌───────┴────────┐
        v                v
┌──────────────┐  ┌──────────────┐
│ LPR Server   │  │ IPP Server   │
│ TCP          │  │ HTTP + IPP   │
└──────┬───────┘  └──────┬───────┘
       │                 │
       └────────┬────────┘
                v
┌───────────────────────────────┐
│ JobStorage                    │
│ raw document + metadata JSON  │
└───────────────────────────────┘
```

## 現状の制約

これは実用化前のMVPです。

1. 待受は `127.0.0.1` のみです。LAN上の別PCからの印刷は受けません。
2. IPPはTLS/IPPS、認証、DNS-SD/mDNS広告を実装していません。
3. IPP Everywhereの全必須属性・全操作を網羅/認証しているわけではありません。
4. IPP/LPRとも1データファイルあたり256MiBを上限としています。
5. MVPではジョブをメモリに読み込んでから保存します。巨大ジョブ向けにはストリーミング保存へ変更するべきです。
6. IPPのCreate-Job + Send-Documentは基本形のみで、複数ドキュメントジョブには未対応です。
7. LPRのRFC 1179にある0バイト長を使ったストリーミング送信には未対応です。
8. 印刷データをPDFへ変換する機能はありません。受信したPDL/ラスタデータをそのまま保存します。

## 実用化するなら次に追加したいもの

- ストリーミングファイル保存
- IPP Everywhere 2.x の適合性向上
- Bonjour / DNS-SD で `_ipp._tcp` を広告
- Windows/macOSプリンター登録をGUIから自動化
- ジョブ履歴一覧、削除、プレビュー
- 保存ファイル形式のフィルタリング
- PDF/PostScript/PWG Rasterなどの解析
- 設定永続化
- 自動起動/バックグラウンドサービス化
