/* インクルードガード */
#pragma once

// アドレス指定
#define SHT31_ADDR 0x45
#define SOFT_RESET_MSB 0x30
#define SOFT_RESET_LSB 0xA2
#define CLEAR_STATUS_REGISTER_MSB 0x30
#define CLEAR_STATUS_REGISTER_LSB 0x41
#define SINGLE_SHOT_HIGH_MSB 0x24
#define SINGLE_SHOT_HIGH_LSB 0x00

class DrTHSensor // classの定義
{
private: // privateはクラス内からしかアクセスできない
    void initAesht3x();

public: // publicはどこからでもアクセス可能
    void init();
    void getTempHumi(double* temperature, double* humidity);
};
